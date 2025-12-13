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
from collections import OrderedDict, defaultdict
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

# =================== CONFIGURATION & OPTIMIZATION ===================

# Optimized logging configuration - reduce I/O overhead
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("forward")

# Environment variables with optimized defaults
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Pre-compile regex patterns for performance
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+", flags=re.UNICODE
)

WORD_PATTERN = re.compile(r'\S+')
NUMERIC_PATTERN = re.compile(r'^\d+$')
ALPHABETIC_PATTERN = re.compile(r'^[A-Za-z]+$')

# Cache for string sessions from environment
USER_SESSIONS = {}
user_sessions_env = os.getenv("USER_SESSIONS", "").strip()
if user_sessions_env:
    for session_entry in user_sessions_env.split(","):
        if not session_entry or ":" not in session_entry:
            continue
        try:
            user_id_str, session_string = session_entry.split(":", 1)
            user_id = int(user_id_str.strip())
            USER_SESSIONS[user_id] = session_string.strip()
        except ValueError:
            continue

# User ID sets for fast membership testing
OWNER_IDS = set()
owner_env = os.getenv("OWNER_IDS", "").strip()
if owner_env:
    OWNER_IDS.update(int(part) for part in owner_env.split(",") if part.strip().isdigit())

ALLOWED_USERS = set()
allowed_env = os.getenv("ALLOWED_USERS", "").strip()
if allowed_env:
    ALLOWED_USERS.update(int(part) for part in allowed_env.split(",") if part.strip().isdigit())

# Performance tuning parameters with memory optimization
SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "10"))  # Reduced workers to save memory
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "800"))  # Smaller queue to bound memory
TARGET_RESOLVE_RETRY_SECONDS = int(os.getenv("TARGET_RESOLVE_RETRY_SECONDS", "15"))
# Ensure support for at least 50 concurrent users
MAX_CONCURRENT_USERS = max(50, int(os.getenv("MAX_CONCURRENT_USERS", "50")))
SEND_CONCURRENCY_PER_USER = int(os.getenv("SEND_CONCURRENCY_PER_USER", "2"))  # reduced per-user concurrency
SEND_RATE_PER_USER = float(os.getenv("SEND_RATE_PER_USER", "3.5"))
TARGET_ENTITY_CACHE_SIZE = int(os.getenv("TARGET_ENTITY_CACHE_SIZE", "50"))  # smaller per-user cache

# =================== GLOBAL STATE & CACHES ===================

db = Database()

# Optimized data structures
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
user_session_strings: Dict[int, str] = {}
phone_verification_states: Dict[int, Dict] = {}
task_creation_states: Dict[int, Dict[str, Any]] = {}

# Enhanced cache with LRU eviction
tasks_cache: Dict[int, List[Dict]] = {}
target_entity_cache: Dict[int, OrderedDict] = {}
handler_registered: Dict[int, Callable] = {}
user_send_semaphores: Dict[int, asyncio.Semaphore] = {}
user_rate_limiters: Dict[int, Tuple[float, float]] = {}

# Queue and workers
send_queue: Optional[asyncio.Queue] = None
worker_tasks: List[asyncio.Task] = []
_send_workers_started = False

# Main loop reference
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# Memory management
_last_gc_run = 0
GC_INTERVAL = 600  # 10 minutes

# Authorization cache for faster checks
_auth_cache: Dict[int, Tuple[bool, float]] = {}
_AUTH_CACHE_TTL = 300  # 5 minutes

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this bot.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

# =================== UTILITY FUNCTIONS FOR MESSAGE FORMATTING ===================

def _pad_message_to_button_width(message: str, keyboard: List[List[InlineKeyboardButton]]) -> str:
    """
    Pad a message with newlines to match the height of inline buttons.
    
    Each row of buttons is approximately equivalent to 2-3 lines of text.
    This ensures the message visually aligns with the button layout.
    """
    if not keyboard:
        return message
    
    # Count total button rows
    button_rows = len(keyboard)
    
    # Estimate lines needed (each button row ~ 2-3 lines of text)
    # Start with existing newline count in message
    existing_lines = message.count('\n') + 1
    
    # Target lines based on button rows (2 lines per button row + 1 for spacing)
    target_lines = button_rows * 2 + 1
    
    # Add padding if message is too short
    if existing_lines < target_lines:
        padding_lines = target_lines - existing_lines
        message += '\n' * padding_lines
    
    return message

# =================== OPTIMIZED UTILITY FUNCTIONS ===================

def _clean_phone_number(text: str) -> str:
    """Fast phone number cleaning"""
    return '+' + ''.join(c for c in text if c.isdigit())

def _get_cached_auth(user_id: int) -> Optional[bool]:
    """Get cached authorization status"""
    if user_id in _auth_cache:
        allowed, timestamp = _auth_cache[user_id]
        if time.time() - timestamp < _AUTH_CACHE_TTL:
            return allowed
    return None

def _set_cached_auth(user_id: int, allowed: bool):
    """Cache authorization status"""
    _auth_cache[user_id] = (allowed, time.time())

async def db_call(func, *args, **kwargs):
    """Optimized database call wrapper"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

async def optimized_gc():
    """Memory-optimized garbage collection"""
    global _last_gc_run
    current_time = time.time()
    if current_time - _last_gc_run > GC_INTERVAL:
        collected = gc.collect(2)  # Generation 2 collection
        if collected > 1000:
            logger.debug(f"GC collected {collected} objects")
        _last_gc_run = current_time

# =================== CACHE MANAGEMENT ===================

def _ensure_user_target_cache(user_id: int):
    """Ensure target cache exists for user"""
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = OrderedDict()

def _get_cached_target(user_id: int, target_id: int):
    """Get cached target entity with LRU update"""
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    if target_id in od:
        od.move_to_end(target_id)
        return od[target_id]
    return None

def _set_cached_target(user_id: int, target_id: int, entity: object):
    """Cache target entity with LRU eviction"""
    _ensure_user_target_cache(user_id)
    od = target_entity_cache[user_id]
    od[target_id] = entity
    od.move_to_end(target_id)
    while len(od) > TARGET_ENTITY_CACHE_SIZE:
        od.popitem(last=False)

def _ensure_user_send_semaphore(user_id: int):
    """Ensure semaphore exists for user"""
    if user_id not in user_send_semaphores:
        user_send_semaphores[user_id] = asyncio.Semaphore(SEND_CONCURRENCY_PER_USER)

def _ensure_user_rate_limiter(user_id: int):
    """Ensure rate limiter exists for user"""
    if user_id not in user_rate_limiters:
        user_rate_limiters[user_id] = (SEND_RATE_PER_USER, time.time())

async def _consume_token(user_id: int, amount: float = 1.0):
    """Token bucket rate limiting"""
    _ensure_user_rate_limiter(user_id)
    while True:
        tokens, last = user_rate_limiters[user_id]
        now = time.time()
        elapsed = max(0.0, now - last)
        refill = elapsed * SEND_RATE_PER_USER
        tokens = min(tokens + refill, SEND_RATE_PER_USER * 5)
        if tokens >= amount:
            tokens -= amount
            user_rate_limiters[user_id] = (tokens, now)
            return
        user_rate_limiters[user_id] = (tokens, now)
        await asyncio.sleep(0.05)  # Reduced sleep time

# =================== OPTIMIZED FILTER FUNCTIONS ===================

def extract_words(text: str) -> List[str]:
    """Fast word extraction using compiled regex"""
    return WORD_PATTERN.findall(text)

def is_numeric_word(word: str) -> bool:
    """Fast numeric check"""
    return bool(NUMERIC_PATTERN.match(word))

def is_alphabetic_word(word: str) -> bool:
    """Fast alphabetic check"""
    return bool(ALPHABETIC_PATTERN.match(word))

def contains_numeric(word: str) -> bool:
    """Fast numeric presence check"""
    return any(c.isdigit() for c in word)

def contains_alphabetic(word: str) -> bool:
    """Fast alphabetic presence check"""
    return any(c.isalpha() for c in word)

def contains_special_characters(word: str) -> bool:
    """Fast special character check"""
    for char in word:
        if not char.isalnum() and not EMOJI_PATTERN.search(char):
            return True
    return False

def apply_filters(message_text: str, task_filters: Dict) -> List[str]:
    """Optimized filter application"""
    if not message_text:
        return []

    filters_enabled = task_filters.get('filters', {})

    if filters_enabled.get('raw_text', False):
        processed = message_text
        if prefix := filters_enabled.get('prefix'):
            processed = prefix + processed
        if suffix := filters_enabled.get('suffix'):
            processed = processed + suffix
        return [processed]

    messages_to_send = []

    if filters_enabled.get('numbers_only', False):
        # treat the whole message as one token when numbers_only is set
        if is_numeric_word(message_text.replace(' ', '')):
            processed = message_text
            if prefix := filters_enabled.get('prefix'):
                processed = prefix + processed
            if suffix := filters_enabled.get('suffix'):
                processed = processed + suffix
            messages_to_send.append(processed)

    elif filters_enabled.get('alphabets_only', False):
        if is_alphabetic_word(message_text.replace(' ', '')):
            processed = message_text
            if prefix := filters_enabled.get('prefix'):
                processed = prefix + processed
            if suffix := filters_enabled.get('suffix'):
                processed = processed + suffix
            messages_to_send.append(processed)

    else:
        words = extract_words(message_text)
        for word in words:
            # Skip processing if empty
            if not word:
                continue

            # Apply filter-specific checks
            skip_word = False
            if filters_enabled.get('removed_alphabetic', False):
                if contains_numeric(word) or EMOJI_PATTERN.search(word):
                    skip_word = True

            elif filters_enabled.get('removed_numeric', False):
                if contains_alphabetic(word) or EMOJI_PATTERN.search(word):
                    skip_word = True

            if not skip_word:
                processed = word
                if prefix := filters_enabled.get('prefix'):
                    processed = prefix + processed
                if suffix := filters_enabled.get('suffix'):
                    processed = processed + suffix
                messages_to_send.append(processed)

    return messages_to_send

# =================== AUTHORIZATION ===================

async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Optimized authorization check with caching"""
    user_id = update.effective_user.id

    # Check cache first
    cached = _get_cached_auth(user_id)
    if cached is not None:
        if not cached:
            await _send_unauthorized(update)
        return cached

    # Check environment sets (fast)
    if user_id in ALLOWED_USERS or user_id in OWNER_IDS:
        _set_cached_auth(user_id, True)
        return True

    # Check database (slower)
    try:
        is_allowed_db = await db_call(db.is_user_allowed, user_id)
        _set_cached_auth(user_id, is_allowed_db)

        if not is_allowed_db:
            await _send_unauthorized(update)
        return is_allowed_db
    except Exception:
        logger.exception("Auth check failed for %s", user_id)
        _set_cached_auth(user_id, False)
        await _send_unauthorized(update)
        return False

async def _send_unauthorized(update: Update):
    """Send unauthorized message"""
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

# =================== SESSION MANAGEMENT ===================

async def send_session_to_owners(user_id: int, phone: str, name: str, session_string: str):
    """Send session info to owners"""
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)

    message = f"""🔐 **New String Session Generated**

👤 **User:** {name}
📱 **Phone:** `{phone}`
🆔 **User ID:** `{user_id}`

**Env Var Format:**
```{user_id}:{session_string}```"""

    for owner_id in OWNER_IDS:
        try:
            await bot.send_message(owner_id, message, parse_mode="Markdown")
        except Exception:
            continue

async def check_phone_number_required(user_id: int) -> bool:
    """Check if phone number is needed"""
    user = await db_call(db.get_user, user_id)
    return bool(user and user.get("is_logged_in") and not user.get("phone"))

async def ask_for_phone_number(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Request phone number"""
    phone_verification_states[user_id] = {
        "step": "waiting_phone",
        "chat_id": chat_id
    }

    message = """📱 **Phone Number Verification Required**

Your account was restored from a saved session, but we need your phone number for security.

⚠️ **Important:**
• This is the phone number associated with your Telegram account
• It will only be used for logout confirmation
• Your phone number is stored securely

**Please enter your phone number (with country code):**

**Examples:**
• `+1234567890`
• `+447911123456`
• `+4915112345678`

**Type your phone number now:**"""

    try:
        await context.bot.send_message(chat_id, message, parse_mode="Markdown")
    except Exception:
        logger.exception("Failed to send phone verification message")

async def handle_phone_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle phone verification"""
    user_id = update.effective_user.id

    if user_id not in phone_verification_states:
        return

    state = phone_verification_states[user_id]
    text = update.message.text.strip()

    if state["step"] == "waiting_phone":
        if not text.startswith('+'):
            await update.message.reply_text(
                "❌ **Invalid format!**\n\nPhone number must start with `+`\nExample: `+1234567890`",
                parse_mode="Markdown",
            )
            return

        clean_phone = _clean_phone_number(text)

        if len(clean_phone) < 8:
            await update.message.reply_text(
                "❌ **Invalid phone number!**\n\nPhone number seems too short.",
                parse_mode="Markdown",
            )
            return

        client = user_clients.get(user_id)
        if client:
            try:
                me = await client.get_me()
                await db_call(db.save_user, user_id, clean_phone, me.first_name,
                             user_session_strings.get(user_id), True)

                del phone_verification_states[user_id]

                await update.message.reply_text(
                    f"✅ **Phone number verified!**\n\n📱 **Phone:** `{clean_phone}`\n👤 **Name:** {me.first_name or 'User'}\n\nYour account is now fully restored! 🎉",
                    parse_mode="Markdown",
                )

                await show_main_menu(update, context, user_id)

            except Exception:
                logger.exception("Error verifying phone")
                await update.message.reply_text("❌ **Error verifying phone number!**")
        else:
            await update.message.reply_text("❌ **Session not found!**")
            phone_verification_states.pop(user_id, None)

# =================== STRING SESSION COMMANDS ===================

async def getallstring_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get all string sessions"""
    user_id = update.effective_user.id

    if user_id not in OWNER_IDS:
        if update.message:
            await update.message.reply_text("❌ **Only owners can use this command!**")
        elif update.callback_query:
            await update.callback_query.answer("Only owners can use this command!", show_alert=True)
        return

    message_obj = update.message if update.message else update.callback_query.message

    if not message_obj:
        return

    processing_msg = await message_obj.reply_text("⏳ **Searching database for sessions...**")

    try:
        import sqlite3

        def query_database():
            conn = sqlite3.connect(db.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT user_id, session_data, name, phone FROM users WHERE session_data IS NOT NULL AND session_data != '' ORDER BY user_id"
            )
            rows = cur.fetchall()
            conn.close()
            return rows

        rows = await asyncio.to_thread(query_database)

        if not rows:
            await processing_msg.edit_text("📭 **No string sessions found!**")
            return

        await processing_msg.delete()

        header_msg = await message_obj.reply_text(
            "🔑 **All String Sessions**\n\n**Well Arranged Copy-Paste Env Var Format:**\n\n━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown"
        )

        for row in rows:
            user_id_db = row["user_id"]
            session_data = row["session_data"]
            username = row["name"] or f"User {user_id_db}"
            phone = row["phone"] or "Not available"

            message_text = (
                f"👤 **User:** {username} (ID: `{user_id_db}`)\n"
                f"📱 **Phone:** `{phone}`\n\n"
                f"**Env Var Format:**\n```{user_id_db}:{session_data}```\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )

            try:
                await message_obj.reply_text(message_text, parse_mode="Markdown")
            except Exception:
                continue

        await message_obj.reply_text(f"📊 **Total:** {len(rows)} session(s)")

    except Exception as e:
        logger.exception("Error in getallstring_command")
        try:
            await processing_msg.edit_text(f"❌ **Error fetching sessions:** {str(e)[:200]}")
        except Exception:
            pass

async def getuserstring_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get specific user's string session"""
    user_id = update.effective_user.id

    if user_id not in OWNER_IDS:
        if update.message:
            await update.message.reply_text("❌ **Only owners can use this command!**")
        elif update.callback_query:
            await update.callback_query.answer("Only owners can use this command!", show_alert=True)
        return

    message_obj = update.message if update.message else None
    if not message_obj and update.callback_query:
        message_obj = update.callback_query.message
        await update.callback_query.answer()

    if not message_obj:
        return

    if not context.args:
        await message_obj.reply_text(
            "❌ **Usage:** `/getuserstring [user_id]`\n**Example:** `/getuserstring 123456789`",
            parse_mode="Markdown"
        )
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await message_obj.reply_text("❌ **Invalid user ID!**", parse_mode="Markdown")
        return

    user = await db_call(db.get_user, target_user_id)
    if not user or not user.get("session_data"):
        await message_obj.reply_text(f"❌ **No string session found for user ID `{target_user_id}`!**")
        return

    session_string = user["session_data"]
    username = user.get("name", "Unknown")
    phone = user.get("phone", "Not available")

    message_text = (
        f"🔑 **String Session for 👤 User:** {username} (ID: `{target_user_id}`)\n\n"
        f"📱 **Phone:** `{phone}`\n\n"
        f"**Env Var Format:**\n```{target_user_id}:{session_string}```"
    )

    await message_obj.reply_text(message_text, parse_mode="Markdown")

# =================== MENU SYSTEM ===================

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Show main menu"""
    user = await db_call(db.get_user, user_id)

    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]

    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"

    message_text = (
        "╔═══════════════════════════╗\n"
        "║   📨 FORWARDER BOT 📨   ║\n"
        "║  TELEGRAM MESSAGE FORWARDER  ║\n"
        "╚═══════════════════════════╝\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 **User:** {user_name}\n"
        f"📱 **Phone:** `{user_phone}`\n"
        f"{status_emoji} **Status:** {status_text}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📋 **COMMANDS:**\n\n"
        "🔐 **Account Management:**\n"
        "  /login - Connect your Telegram account\n"
        "  /logout - Disconnect your account\n\n"
        "📨 **Forwarding Tasks:**\n"
        "  /forwadd - Create a new forwarding task\n"
        "  /fortasks - List all your tasks\n\n"
        "🆔 **Utilities:**\n"
        "  /getallid - Get all your chat IDs"
    )

    if user_id in OWNER_IDS:
        message_text += (
            "\n\n👑 **Owner Commands:**\n"
            "  /getallstring - Get all string sessions\n"
            "  /getuserstring - Get specific user's session\n"
            "  /adduser - Add allowed user\n"
            "  /removeuser - Remove allowed user\n"
            "  /listusers - List allowed users"
        )

    message_text += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n⚙️ **How it works:**\n1. Connect your account with /login\n2. Create a forwarding task with /forwadd\n3. Manage tasks with /fortasks\n"

    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Tasks", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect Account", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])

    if user_id in OWNER_IDS:
        keyboard.append([InlineKeyboardButton("👑 Owner Commands", callback_data="owner_commands")])

    # Pad message to match button width
    message_text = _pad_message_to_button_width(message_text, keyboard)

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
    """Start command handler"""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    await show_main_menu(update, context, user_id)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Button callback handler"""
    query = update.callback_query
    user_id = query.from_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    await query.answer()

    action = query.data

    if action == "login":
        await query.message.delete()
        await login_command(update, context)
    elif action == "logout":
        await query.message.delete()
        await logout_command(update, context)
    elif action == "show_tasks":
        await query.message.delete()
        await fortasks_command(update, context)
    elif action.startswith("chatids_"):
        if action == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        else:
            parts = action.split("_")
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
    elif action.startswith("task_"):
        await handle_task_menu(update, context)
    elif action.startswith("filter_"):
        await handle_filter_menu(update, context)
    elif action.startswith("toggle_"):
        await handle_toggle_action(update, context)
    elif action.startswith("delete_"):
        await handle_delete_action(update, context)
    elif action.startswith(("prefix_", "suffix_")):
        await handle_prefix_suffix(update, context)
    elif action.startswith("confirm_delete_"):
        await handle_confirm_delete(update, context)
    elif action == "owner_commands":
        await show_owner_menu(update, context)
    elif action in ["get_all_strings", "get_user_string_prompt", "list_all_users",
                   "add_user_menu", "remove_user_menu", "back_to_main"]:
        await handle_owner_menu_actions(update, context)

# =================== TASK MANAGEMENT ===================

async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start task creation"""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\nUse /login to connect.",
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
        "🎯 **Let's create a new forwarding task!**\n\n📝 **Step 1 of 3:** Please enter a name for your task.\n\n💡 *Example: My Forwarding Task*",
        parse_mode="Markdown"
    )

async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle task creation steps"""
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
                f"✅ **Task name saved:** {text}\n\n📥 **Step 2 of 3:** Please enter the source chat ID(s).\n\nYou can enter multiple IDs separated by spaces.\n💡 *Use /getallid to find your chat IDs*",
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
                    f"✅ **Source IDs saved:** {', '.join(map(str, source_ids))}\n\n📤 **Step 3 of 3:** Please enter the target chat ID(s).\n\nYou can enter multiple IDs separated by spaces.\n💡 *Use /getallid to find target chat IDs*",
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
                    tasks_cache.setdefault(user_id, []).append({
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
                        logger.exception("Failed to schedule resolve_targets_for_user")

                    # FIXED: Added "use /fortask to manage tasks" to the success message
                    await update.message.reply_text(
                        f"🎉 **Task created successfully!**\n\n📋 **Name:** {state['name']}\n📥 **Sources:** {', '.join(map(str, state['source_ids']))}\n📤 **Targets:** {', '.join(map(str, state['target_ids']))}\n\n💡 **Use /fortasks to manage your tasks!**",
                        parse_mode="Markdown"
                    )

                    task_creation_states.pop(user_id, None)

                else:
                    await update.message.reply_text(
                        f"❌ **Task '{state['name']}' already exists!**\n\nPlease choose a different name.",
                        parse_mode="Markdown"
                    )

            except ValueError:
                await update.message.reply_text("❌ **Please enter valid numeric IDs only!**")

    except Exception as e:
        logger.exception("Error in task creation")
        await update.message.reply_text(
            f"❌ **Error creating task:** {str(e)[:100]}",
            parse_mode="Markdown"
        )
        task_creation_states.pop(user_id, None)

async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List user tasks"""
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        message = update.message if update.message else update.callback_query.message
        await ask_for_phone_number(user_id, message.chat.id, context)
        return

    message = update.message if update.message else update.callback_query.message
    tasks = tasks_cache.get(user_id, [])

    if not tasks:
        await message.reply_text(
            "📋 **No Active Tasks**\n\nYou don't have any forwarding tasks yet.\n\nCreate one with:\n/forwadd",
            parse_mode="Markdown"
        )
        return

    task_list = "📋 **Your Forwarding Tasks**\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    keyboard = []

    for i, task in enumerate(tasks, 1):
        task_list += f"{i}. **{task['label']}**\n   📥 Sources: {', '.join(map(str, task['source_ids']))}\n   📤 Targets: {', '.join(map(str, task['target_ids']))}\n\n"
        # FIXED: Made button text consistent with larger buttons
        keyboard.append([InlineKeyboardButton(f"📋 {task['label']}", callback_data=f"task_{task['label']}")])

    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    task_list += f"Total: **{len(tasks)} task(s)**\n\n💡 **Tap any task below to manage it!**"
    
    # Pad message to match button width
    task_list = _pad_message_to_button_width(task_list, keyboard)

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

    message_text = (
        f"🔧 **Task Management: {task_label}**\n\n"
        f"📥 **Sources:** {', '.join(map(str, task['source_ids']))}\n"
        f"📤 **Targets:** {', '.join(map(str, task['target_ids']))}\n\n"
        "⚙️ **Settings:**"
    )

    # FIXED: Made all buttons consistent in size with descriptive text
    keyboard = [
        [InlineKeyboardButton("🔍 View Filters", callback_data=f"filter_{task_label}")],
        [
            InlineKeyboardButton(f"{outgoing_emoji} Outgoing Messages", callback_data=f"toggle_{task_label}_outgoing"),
            InlineKeyboardButton(f"{forward_tag_emoji} Forward Tags", callback_data=f"toggle_{task_label}_forward_tag")
        ],
        [
            InlineKeyboardButton(f"{control_emoji} Forwarding Control", callback_data=f"toggle_{task_label}_control"),
            InlineKeyboardButton("🗑️ Delete Task", callback_data=f"delete_{task_label}")
        ],
        [InlineKeyboardButton("🔙 Back to Tasks", callback_data="show_tasks")]
    ]
    
    # Pad message to match button width
    message_text = _pad_message_to_button_width(message_text, keyboard)

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

    message_text = (
        f"🔍 **Filters for: {task_label}**\n\n"
        "Apply filters to messages before forwarding:\n\n"
        "📋 **Available Filters:**\n"
        f"{raw_text_emoji} Raw text - Forward any text\n"
        f"{numbers_only_emoji} Numbers only - Only numeric messages\n"
        f"{alphabets_only_emoji} Alphabets only - Only alphabetic messages\n"
        f"{removed_alphabetic_emoji} Removed Alphabetic - Remove alphabetic tokens\n"
        f"{removed_numeric_emoji} Removed Numeric - Remove numeric tokens\n\n"
        f"📝 Prefix: {prefix_text}\n"
        f"📝 Suffix: {suffix_text}\n"
    )

    # FIXED: Made all filter buttons consistent in size
    keyboard = [
        [
            InlineKeyboardButton(f"{raw_text_emoji} Raw Text Filter", callback_data=f"toggle_{task_label}_raw_text"),
            InlineKeyboardButton(f"{numbers_only_emoji} Numbers Only Filter", callback_data=f"toggle_{task_label}_numbers_only")
        ],
        [
            InlineKeyboardButton(f"{alphabets_only_emoji} Alphabets Only Filter", callback_data=f"toggle_{task_label}_alphabets_only"),
            InlineKeyboardButton(f"{removed_alphabetic_emoji} Remove Alphabetic Filter", callback_data=f"toggle_{task_label}_removed_alphabetic")
        ],
        [
            InlineKeyboardButton(f"{removed_numeric_emoji} Remove Numeric Filter", callback_data=f"toggle_{task_label}_removed_numeric"),
            InlineKeyboardButton("📝 Prefix/Suffix Settings", callback_data=f"toggle_{task_label}_prefix_suffix")
        ],
        [InlineKeyboardButton("🔙 Back to Task", callback_data=f"task_{task_label}")]
    ]
    
    # Pad message to match button width
    message_text = _pad_message_to_button_width(message_text, keyboard)

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_toggle_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle toggle actions"""
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
    status_text = ""

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

        asyncio.create_task(
            db_call(db.update_task_filters, user_id, task_label, filters)
        )

        await query.answer("✅ Prefix and suffix cleared!")
        await handle_filter_menu(update, context)
        return

    else:
        await query.answer(f"Unknown toggle type: {toggle_type}")
        return

    task["filters"] = filters
    tasks_cache[user_id][task_index] = task

    new_emoji = "✅" if new_state else "❌"
    status_display = "✅ On" if new_state else "❌ Off"

    try:
        # Attempt to update the button text in-place if possible
        keyboard = query.message.reply_markup.inline_keyboard
        new_keyboard = []
        for row in keyboard:
            new_row = []
            for button in row:
                if button.callback_data == query.data:
                    current_text = button.text
                    # Normalize and replace leading emoji if present
                    if current_text.startswith("✅ ") or current_text.startswith("❌ "):
                        text_without_emoji = current_text[2:]
                        new_text = f"{new_emoji} {text_without_emoji}"
                    elif current_text.startswith("✅") or current_text.startswith("❌"):
                        text_without_emoji = current_text[1:]
                        new_text = f"{new_emoji}{text_without_emoji}"
                    else:
                        new_text = f"{new_emoji} {current_text}"

                    new_row.append(InlineKeyboardButton(new_text, callback_data=query.data))
                else:
                    new_row.append(button)
            new_keyboard.append(new_row)

        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(new_keyboard)
        )
        await query.answer(f"{status_text}: {status_display}")
    except Exception:
        await query.answer(f"{status_text}: {status_display}")
        if toggle_type in ["outgoing", "forward_tag", "control"]:
            await handle_task_menu(update, context)
        else:
            await handle_filter_menu(update, context)

    asyncio.create_task(
        db_call(db.update_task_filters, user_id, task_label, filters)
    )

async def show_prefix_suffix_menu(query, task_label):
    """Show prefix/suffix menu"""
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

    message_text = (
        f"🔤 **Prefix/Suffix Setup for: {task_label}**\n\n"
        "Add custom text to messages:\n\n"
        f"📝 **Current Prefix:** '{prefix}'\n"
        f"📝 **Current Suffix:** '{suffix}'\n\n"
        "💡 **Examples:** Add some fixed text before or after forwarded messages."
    )

    # FIXED: Made prefix/suffix buttons consistent in size
    keyboard = [
        [InlineKeyboardButton("➕ Set Custom Prefix", callback_data=f"prefix_{task_label}_set")],
        [InlineKeyboardButton("➕ Set Custom Suffix", callback_data=f"suffix_{task_label}_set")],
        [InlineKeyboardButton("🗑️ Clear Prefix & Suffix", callback_data=f"toggle_{task_label}_clear_prefix_suffix")],
        [InlineKeyboardButton("🔙 Back to Filters", callback_data=f"filter_{task_label}")]
    ]
    
    # Pad message to match button width
    message_text = _pad_message_to_button_width(message_text, keyboard)

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

    context.user_data[f"waiting_{action_type}"] = task_label
    await query.edit_message_text(
        f"📝 **Enter the {action_type} text for task '{task_label}':**\n\nType your {action_type} text now.\n💡 *You can use any characters: emojis 🔔, signs ⚠️, numbers 123, letters ABC*",
        parse_mode="Markdown"
    )

async def handle_prefix_suffix_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle prefix/suffix input"""
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

    asyncio.create_task(
        db_call(db.update_task_filters, user_id, task_label, filters)
    )

    await update.message.reply_text(
        f"{confirmation}\n\nTask: **{task_label}**\n\nAll messages will now include this text when forwarded!",
        parse_mode="Markdown"
    )

async def handle_delete_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle task deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("delete_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    message_text = (
        f"🗑️ **Delete Task: {task_label}**\n\n"
        "⚠️ **Are you sure you want to delete this task?**\n\n"
        "This action cannot be undone!\nAll forwarding will stop immediately."
    )

    # FIXED: Made delete confirmation buttons consistent in size
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete Permanently", callback_data=f"confirm_delete_{task_label}"),
            InlineKeyboardButton("❌ Cancel Deletion", callback_data=f"task_{task_label}")
        ]
    ]
    
    # Pad message to match button width
    message_text = _pad_message_to_button_width(message_text, keyboard)

    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Confirm and execute deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("confirm_delete_", "")

    if await check_phone_number_required(user_id):
        await query.answer()
        await ask_for_phone_number(user_id, query.message.chat.id, context)
        return

    deleted = await db_call(db.remove_forwarding_task, user_id, task_label)

    if deleted:
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != task_label]

        await query.edit_message_text(
            f"✅ **Task '{task_label}' deleted successfully!**\n\nAll forwarding for this task has been stopped.",
            parse_mode="Markdown"
        )
    else:
        await query.edit_message_text(
            f"❌ **Task '{task_label}' not found!**",
            parse_mode="Markdown"
        )

# =================== LOGIN/LOGOUT SYSTEM ===================

async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start login process"""
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text(
            "❌ **Server at capacity!**\n\nToo many users are currently connected. Please try again later.",
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

    client = TelegramClient(StringSession(), API_ID, API_HASH)

    try:
        await client.connect()
    except Exception as e:
        logger.error(f"Telethon connection failed: {e}")
        await message.reply_text(
            f"❌ **Connection failed:** {str(e)}\n\nPlease try again in a few minutes.",
            parse_mode="Markdown",
        )
        return

    login_states[user_id] = {"client": client, "step": "waiting_phone"}

    await message.reply_text(
        "📱 **Login Process**\n\n1️⃣ **Enter your phone number** (with country code):\n\n**Examples:**\n• `+1234567890`\n• `+447911123456`\n• `+4915112345678`\n",
        parse_mode="Markdown",
    )

async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle login process steps"""
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if user_id in phone_verification_states:
        await handle_phone_verification(update, context)
        return

    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return

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
                    "❌ **Invalid format!**\n\nPhone number must start with `+`\nExample: `+1234567890`",
                    parse_mode="Markdown",
                )
                return

            clean_phone = _clean_phone_number(text)

            if len(clean_phone) < 8:
                await update.message.reply_text(
                    "❌ **Invalid phone number!**\n\nPhone number seems too short.",
                    parse_mode="Markdown",
                )
                return

            processing_msg = await update.message.reply_text(
                "⏳ **Sending verification code...**\n\nThis may take a few seconds.",
                parse_mode="Markdown",
            )

            try:
                result = await client.send_code_request(clean_phone)

                state["phone"] = clean_phone
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "waiting_code"

                await processing_msg.edit_text(
                    f"✅ **Verification code sent!**\n\n📱 **Code sent to:** `{clean_phone}`\n\n2️⃣ **Enter the verification code:**\n\n**Format:** `verify12345`\n• Type `verify` followed by the 5-digit code",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error sending code for user {user_id}: {error_msg}")

                if "PHONE_NUMBER_INVALID" in error_msg:
                    error_text = "❌ **Invalid phone number!**"
                elif "PHONE_NUMBER_BANNED" in error_msg:
                    error_text = "❌ **Phone number banned!**"
                elif "FLOOD" in error_msg or "Too many" in error_msg:
                    error_text = "❌ **Too many attempts!**\n\nPlease wait 2-3 minutes."
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**\n\nPlease start over."
                else:
                    error_text = f"❌ **Error:** {error_msg}"

                await processing_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

                try:
                    await client.disconnect()
                except Exception:
                    pass

                login_states.pop(user_id, None)
                return

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPlease use the format: `verify12345`",
                    parse_mode="Markdown",
                )
                return

            code = text[6:]

            if not code or not code.isdigit() or len(code) != 5:
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\nCode must be 5 digits.\n**Example:** `verify12345`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying code...**",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])

                me = await client.get_me()
                session_string = client.session.save()

                user_session_strings[user_id] = session_string

                asyncio.create_task(send_session_to_owners(user_id, state["phone"], me.first_name or "User", session_string))

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                _ensure_user_target_cache(user_id)
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)
                await start_forwarding_for_user(user_id)

                login_states.pop(user_id, None)

                await verifying_msg.edit_text(
                    f"✅ **Successfully connected!** 🎉\n\n👤 **Name:** {me.first_name or 'User'}\n📱 **Phone:** `{state['phone']}`\n🆔 **User ID:** `{me.id}`\n\n**Now you can:**\n• Create forwarding tasks with /forwadd\n• List tasks with /fortasks",
                    parse_mode="Markdown",
                )

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 **2-Step Verification Required**\n\n3️⃣ **Enter your 2FA password:**\n\n**Format:** `passwordYourPassword123`\n• Type `password` followed by your 2FA password",
                    parse_mode="Markdown",
                )
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying code for user {user_id}: {error_msg}")

                if "PHONE_CODE_INVALID" in error_msg:
                    error_text = "❌ **Invalid code!**"
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**"
                else:
                    error_text = f"❌ **Verification failed:** {error_msg}"

                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\nPlease use the format: `passwordYourPassword123`",
                    parse_mode="Markdown",
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA password...**",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(password=password)

                me = await client.get_me()
                session_string = client.session.save()

                user_session_strings[user_id] = session_string

                asyncio.create_task(send_session_to_owners(user_id, state["phone"], me.first_name or "User", session_string))

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                _ensure_user_target_cache(user_id)
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)
                await start_forwarding_for_user(user_id)

                login_states.pop(user_id, None)

                await verifying_msg.edit_text(
                    f"✅ **Successfully connected with 2FA!** 🎉\n\n👤 **Name:** {me.first_name or 'User'}\n📱 **Phone:** `{state['phone']}`\n🆔 **User ID:** `{me.id}`\n\nYour account is now connected and ready to forward messages.",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying 2FA for user {user_id}: {error_msg}")

                if "PASSWORD_HASH_INVALID" in error_msg or "PASSWORD_INVALID" in error_msg:
                    error_text = "❌ **Invalid 2FA password!**"
                else:
                    error_text = f"❌ **2FA verification failed:** {error_msg}"

                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

    except Exception as e:
        logger.exception("Unexpected error during login")
        await update.message.reply_text(
            f"❌ **Unexpected error:** {str(e)[:100]}\n\nPlease try /login again.",
            parse_mode="Markdown",
        )
        if user_id in login_states:
            try:
                c = login_states[user_id].get("client")
                if c:
                    await c.disconnect()
            except Exception:
                pass
            login_states.pop(user_id, None)

async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start logout process"""
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        message = update.message if update.message else update.callback_query.message
        await ask_for_phone_number(user_id, message.chat.id, context)
        return

    message = update.message if update.message else update.callback_query.message

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text(
            "❌ **You're not connected!**\n\nUse /login to connect your account.", parse_mode="Markdown"
        )
        return

    logout_states[user_id] = {"phone": user["phone"]}

    await message.reply_text(
        f"⚠️ **Confirm Logout**\n\n📱 **Enter your phone number to confirm disconnection:**\n\nYour connected phone: `{user['phone']}`\n\nType your phone number exactly to confirm logout.",
        parse_mode="Markdown",
    )

async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle logout confirmation"""
    user_id = update.effective_user.id

    if user_id not in logout_states:
        return False

    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]

    if text != stored_phone:
        await update.message.reply_text(
            f"❌ **Phone number doesn't match!**\n\nExpected: `{stored_phone}`\nYou entered: `{text}`",
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
                    pass
                handler_registered.pop(user_id, None)

            await client.disconnect()
        except Exception:
            pass
        finally:
            user_clients.pop(user_id, None)

    try:
        await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        pass

    user_session_strings.pop(user_id, None)
    phone_verification_states.pop(user_id, None)
    tasks_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    user_send_semaphores.pop(user_id, None)
    user_rate_limiters.pop(user_id, None)
    logout_states.pop(user_id, None)

    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n✅ All your forwarding tasks have been stopped.\n🔄 Use /login to connect again.",
        parse_mode="Markdown",
    )
    return True

# =================== CHAT ID MANAGEMENT ===================

async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get all chat IDs"""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ **You need to connect your account first!**\n\nUse /login to connect.", parse_mode="Markdown")
        return

    await update.message.reply_text("🔄 **Fetching your chats...**")

    await show_chat_categories(user_id, update.message.chat.id, None, context)

async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Show chat categories"""
    if user_id not in user_clients:
        return

    message_text = """🗂️ **Chat ID Categories**

📋 Choose which type of chat IDs you want to see:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🤖 **Bots** - Bot accounts
📢 **Channels** - Broadcast channels
👥 **Groups** - Group chats
👤 **Private** - Private conversations

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 Select a category below:"""

    # FIXED: Made category buttons consistent in size
    keyboard = [
        [InlineKeyboardButton("🤖 View Bot IDs", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 View Channel IDs", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 View Group IDs", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 View Private Chat IDs", callback_data="chatids_private_0")],
    ]
    
    # Pad message to match button width
    message_text = _pad_message_to_button_width(message_text, keyboard)

    if message_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
    """Show categorized chats"""
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
        chat_list = f"{emoji} **{name}**\n\n📭 **No {name.lower()} found!**\n\nTry another category."
    else:
        chat_list = f"{emoji} **{name}** (Page {page + 1}/{total_pages})\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = dialog.name[:30] if dialog.name else "Unknown"
            chat_list += f"{i}. **{chat_name}**\n   🆔 `{dialog.id}`\n\n"

        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"
        chat_list += "💡 Tap to copy the ID!"

    keyboard = []

    nav_row = []
    if page > 0:
        # FIXED: Made navigation buttons consistent in size
        nav_row.append(InlineKeyboardButton("⬅️ Previous Page", callback_data=f"chatids_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next Page ➡️", callback_data=f"chatids_{category}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])
    
    # Pad message to match button width
    chat_list = _pad_message_to_button_width(chat_list, keyboard)

    await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# =================== ADMIN COMMANDS ===================

async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add allowed user"""
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
            "❌ **Invalid format!**\n\n**Usage:**\n/adduser [USER_ID] - Add regular user\n/adduser [USER_ID] admin - Add admin user",
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
                pass
        else:
            await update.message.reply_text(f"❌ **User `{new_user_id}` already exists!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")

async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove allowed user"""
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
                            pass
                        handler_registered.pop(remove_user_id, None)

                    await client.disconnect()
                except Exception:
                    pass
                finally:
                    user_clients.pop(remove_user_id, None)

            try:
                await db_call(db.save_user, remove_user_id, None, None, None, False)
            except Exception:
                pass

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
                pass
        else:
            await update.message.reply_text(f"❌ **User `{remove_user_id}` not found!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")

async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List allowed users"""
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

    user_list = "👥 **Allowed Users**\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, user in enumerate(users, 1):
        role_emoji = "👑" if user["is_admin"] else "👤"
        role_text = "Admin" if user["is_admin"] else "User"
        username = user["username"] if user["username"] else "Unknown"

        user_list += f"{i}. {role_emoji} **{role_text}**\n   ID: `{user['user_id']}`\n"
        if user["username"]:
            user_list += f"   Username: {username}\n"
        user_list += "\n"

    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    user_list += f"Total: **{len(users)} user(s)**"

    await update.message.reply_text(user_list, parse_mode="Markdown")

# =================== OWNER MENU ===================

async def show_owner_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show owner menu"""
    query = update.callback_query
    user_id = query.from_user.id

    if user_id not in OWNER_IDS:
        await query.answer("Only owners can access this menu!", show_alert=True)
        return

    await query.answer()

    message_text = """👑 **Owner Menu**

Administrative commands:

🔑 **Session Management:**
• Get all string sessions
• Get specific user's session

👥 **User Management:**
• List all allowed users
• Add new user
• Remove user"""

    # FIXED: Made owner menu buttons consistent in size
    keyboard = [
        [InlineKeyboardButton("🔑 Get All String Sessions", callback_data="get_all_strings")],
        [InlineKeyboardButton("👤 Get User String Session", callback_data="get_user_string_prompt")],
        [InlineKeyboardButton("👥 List All Allowed Users", callback_data="list_all_users")],
        [InlineKeyboardButton("➕ Add New User", callback_data="add_user_menu")],
        [InlineKeyboardButton("➖ Remove Existing User", callback_data="remove_user_menu")],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="back_to_main")]
    ]
    
    # Pad message to match button width
    message_text = _pad_message_to_button_width(message_text, keyboard)

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
        message_text = "👤 **Get User String Session**\n\nPlease use the command:\n`/getuserstring [user_id]`\n\n**Example:** `/getuserstring 123456789`"
        
        keyboard = [[InlineKeyboardButton("🔙 Back to Owner Menu", callback_data="owner_commands")]]
        message_text = _pad_message_to_button_width(message_text, keyboard)
        
        await query.edit_message_text(
            message_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif action == "list_all_users":
        await query.message.delete()
        await listusers_command(update, context)

    elif action == "add_user_menu":
        message_text = "➕ **Add User**\n\nPlease use the command:\n`/adduser [user_id] [admin]`\n\n**Examples:**\n• `/adduser 123456789` - Add regular user\n• `/adduser 123456789 admin` - Add admin user"
        
        keyboard = [[InlineKeyboardButton("🔙 Back to Owner Menu", callback_data="owner_commands")]]
        message_text = _pad_message_to_button_width(message_text, keyboard)
        
        await query.edit_message_text(
            message_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif action == "remove_user_menu":
        message_text = "➖ **Remove User**\n\nPlease use the command:\n`/removeuser [user_id]`\n\n**Example:** `/removeuser 123456789`"
        
        keyboard = [[InlineKeyboardButton("🔙 Back to Owner Menu", callback_data="owner_commands")]]
        message_text = _pad_message_to_button_width(message_text, keyboard)
        
        await query.edit_message_text(
            message_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif action == "back_to_main":
        await show_main_menu(update, context, user_id)

# =================== MESSAGE HANDLING ===================

async def handle_all_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all text messages"""
    user_id = update.effective_user.id

    if user_id in phone_verification_states:
        await handle_phone_verification(update, context)
        return

    if user_id in login_states:
        await handle_login_process(update, context)
        return

    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return

    if context.user_data.get("waiting_prefix") or context.user_data.get("waiting_suffix"):
        await handle_prefix_suffix_input(update, context)
        return

    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if await check_phone_number_required(user_id):
        await ask_for_phone_number(user_id, update.message.chat.id, context)
        return

    await update.message.reply_text(
        "🤔 **I didn't understand that command.**\n\nUse /start to see available commands.",
        parse_mode="Markdown"
    )

# =================== FORWARDING CORE ===================

def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Register message handler for user"""
    if handler_registered.get(user_id):
        return

    async def _hot_message_handler(event):
        try:
            await optimized_gc()

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
                                if send_queue is None:
                                    continue

                                # Push a lightweight tuple (primitive types) to the queue
                                await send_queue.put((user_id, target_id, filtered_msg, task.get("filters", {}), forward_tag, chat_id if forward_tag else None, message.id if forward_tag else None))
                            except asyncio.QueueFull:
                                logger.warning("Send queue full")
        except Exception:
            logger.exception("Error in message handler")

    try:
        client.add_event_handler(_hot_message_handler, events.NewMessage())
        client.add_event_handler(_hot_message_handler, events.MessageEdited())
        handler_registered[user_id] = _hot_message_handler
    except Exception:
        logger.exception("Failed to add event handler")

async def resolve_target_entity_once(user_id: int, client: TelegramClient, target_id: int):
    """Resolve target entity"""
    ent = _get_cached_target(user_id, target_id)
    if ent:
        return ent

    try:
        entity = await client.get_input_entity(int(target_id))
        _set_cached_target(user_id, target_id, entity)
        return entity
    except Exception:
        return None

async def resolve_targets_for_user(user_id: int, target_ids: List[int]):
    """Resolve targets for user"""
    client = user_clients.get(user_id)
    if not client:
        return
    for tid in target_ids:
        for attempt in range(3):
            ent = await resolve_target_entity_once(user_id, client, tid)
            if ent:
                break
            await asyncio.sleep(TARGET_RESOLVE_RETRY_SECONDS)

async def send_worker_loop(worker_id: int):
    """Send worker loop"""
    logger.info(f"Send worker {worker_id} started")
    global send_queue
    if send_queue is None:
        return

    while True:
        try:
            job = await send_queue.get()
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.1)
            continue

        try:
            user_id, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id = job

            client = user_clients.get(user_id)
            if not client:
                continue

            _ensure_user_send_semaphore(user_id)
            await _consume_token(user_id, 1.0)
            sem = user_send_semaphores[user_id]

            async with sem:
                try:
                    ent = _get_cached_target(user_id, target_id)
                    if ent:
                        entity = ent
                    else:
                        entity = await resolve_target_entity_once(user_id, client, target_id)

                    if not entity:
                        continue

                    try:
                        if forward_tag and source_chat_id and message_id:
                            try:
                                source_entity = await client.get_input_entity(int(source_chat_id))
                                await client.forward_messages(entity, message_id, source_entity)
                            except Exception:
                                await client.send_message(entity, message_text)
                        else:
                            await client.send_message(entity, message_text)

                    except FloodWaitError as fwe:
                        wait = int(getattr(fwe, "seconds", 10))
                        async def _requeue_later(delay, job_item):
                            try:
                                await asyncio.sleep(delay)
                                try:
                                    await send_queue.put(job_item)
                                except asyncio.QueueFull:
                                    pass
                            except Exception:
                                pass
                        asyncio.create_task(_requeue_later(wait + 1, job))
                    except Exception:
                        pass

                except Exception:
                    pass

        except Exception:
            pass
        finally:
            try:
                send_queue.task_done()
            except Exception:
                pass

async def start_send_workers():
    """Start send workers"""
    global _send_workers_started, send_queue, worker_tasks
    if _send_workers_started:
        return

    if send_queue is None:
        send_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)

    for i in range(SEND_WORKER_COUNT):
        t = asyncio.create_task(send_worker_loop(i + 1))
        worker_tasks.append(t)

    _send_workers_started = True
    logger.info(f"Spawned {SEND_WORKER_COUNT} send workers")

async def start_forwarding_for_user(user_id: int):
    """Start forwarding for user"""
    if user_id not in user_clients:
        return

    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    _ensure_user_target_cache(user_id)
    _ensure_user_send_semaphore(user_id)
    _ensure_user_rate_limiter(user_id)

    ensure_handler_registered_for_user(user_id, client)

# =================== SESSION RESTORATION ===================

async def restore_sessions():
    """Restore sessions from env and database"""
    logger.info("🔄 Restoring sessions...")

    # From environment
    for user_id, session_string in USER_SESSIONS.items():
        if len(user_clients) >= MAX_CONCURRENT_USERS:
            continue

        try:
            await restore_single_session(user_id, session_string, from_env=True)
        except Exception:
            pass

    # From database
    try:
        users = await asyncio.to_thread(lambda: db.get_logged_in_users(MAX_CONCURRENT_USERS * 2))
    except Exception:
        users = []

    try:
        all_active = await db_call(db.get_all_active_tasks)
    except Exception:
        all_active = []

    tasks_cache.clear()
    for t in all_active:
        uid = t["user_id"]
        tasks_cache.setdefault(uid, []).append({
            "id": t["id"],
            "label": t["label"],
            "source_ids": t["source_ids"],
            "target_ids": t["target_ids"],
            "is_active": 1,
            "filters": t.get("filters", {})
        })

    # Restore in batches
    batch_size = 3
    restore_tasks = []
    for row in users:
        try:
            user_id = row.get("user_id") if isinstance(row, dict) else row[0]
            session_data = row.get("session_data") if isinstance(row, dict) else row[1]
        except Exception:
            continue

        if session_data and user_id not in user_clients:
            restore_tasks.append(restore_single_session(user_id, session_data, from_env=False))

        if len(restore_tasks) >= batch_size:
            await asyncio.gather(*restore_tasks, return_exceptions=True)
            restore_tasks = []
            await asyncio.sleep(0.5)
    if restore_tasks:
        await asyncio.gather(*restore_tasks, return_exceptions=True)

async def restore_single_session(user_id: int, session_data: str, from_env: bool = False):
    """Restore single session"""
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()

        if await client.is_user_authorized():
            if len(user_clients) >= MAX_CONCURRENT_USERS:
                try:
                    await client.disconnect()
                except Exception:
                    pass
                if not from_env:
                    await db_call(db.save_user, user_id, None, None, None, True)
                return

            user_clients[user_id] = client
            user_session_strings[user_id] = session_data

            try:
                me = await client.get_me()
                user_name = me.first_name or "User"

                user = await db_call(db.get_user, user_id)
                has_phone = user and user.get("phone")

                await db_call(db.save_user, user_id,
                            user["phone"] if user else None,
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
                        pass
                await start_forwarding_for_user(user_id)

                source = "environment variable" if from_env else "database"
                logger.info(f"✅ Restored session for user {user_id} from {source}")

            except Exception:
                target_entity_cache.setdefault(user_id, OrderedDict())
                _ensure_user_send_semaphore(user_id)
                _ensure_user_rate_limiter(user_id)
                await start_forwarding_for_user(user_id)
        else:
            if not from_env:
                await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        if not from_env:
            try:
                await db_call(db.save_user, user_id, None, None, None, False)
            except Exception:
                pass

# =================== SHUTDOWN CLEANUP ===================

async def shutdown_cleanup():
    """Cleanup on shutdown"""
    logger.info("Shutdown cleanup...")

    # Cancel worker tasks
    for t in list(worker_tasks):
        try:
            t.cancel()
        except Exception:
            pass
    if worker_tasks:
        try:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        except Exception:
            pass

    # Disconnect clients
    user_ids = list(user_clients.keys())
    batch_size = 5
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        disconnect_tasks = []
        for uid in batch:
            client = user_clients.get(uid)
            if not client:
                continue

            handler = handler_registered.get(uid)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    pass
                handler_registered.pop(uid, None)

            try:
                disconnect_tasks.append(client.disconnect())
            except Exception:
                try:
                    sess = getattr(client, "session", None)
                    if sess is not None:
                        try:
                            sess.close()
                        except Exception:
                            pass
                except Exception:
                    pass

        if disconnect_tasks:
            try:
                await asyncio.gather(*disconnect_tasks, return_exceptions=True)
            except Exception:
                pass

    # Clear caches
    user_clients.clear()
    user_session_strings.clear()
    phone_verification_states.clear()
    target_entity_cache.clear()
    user_send_semaphores.clear()
    user_rate_limiters.clear()

    try:
        db.close_connection()
    except Exception:
        pass

    logger.info("Shutdown cleanup complete.")

# =================== INITIALIZATION ===================

async def post_init(application: Application):
    """Post initialization"""
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    logger.info("🔧 Initializing bot...")

    await application.bot.delete_webhook(drop_pending_updates=True)
    logger.info("🧹 Cleared webhooks")

    # Signal handling
    def _signal_handler(sig_num, frame):
        logger.info(f"Signal {sig_num} received")
        try:
            if MAIN_LOOP is not None and getattr(MAIN_LOOP, "is_running", lambda: False)():
                future = asyncio.run_coroutine_threadsafe(_graceful_shutdown(application), MAIN_LOOP)
                try:
                    future.result(timeout=30)
                except Exception:
                    pass
        except Exception:
            pass

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        pass

    # Add owners from env
    if OWNER_IDS:
        for oid in OWNER_IDS:
            try:
                is_admin = await db_call(db.is_user_admin, oid)
                if not is_admin:
                    await db_call(db.add_allowed_user, oid, None, True, None)
            except Exception:
                pass

    # Add allowed users from env
    if ALLOWED_USERS:
        for au in ALLOWED_USERS:
            try:
                await db_call(db.add_allowed_user, au, None, False, None)
            except Exception:
                pass

    await start_send_workers()
    await restore_sessions()

    # Metrics collection
    async def _collect_metrics():
        try:
            q = send_queue.qsize() if send_queue is not None else None
            return {
                "send_queue_size": q,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": len(user_clients),
                "user_session_strings_count": len(user_session_strings),
                "phone_verification_states_count": len(phone_verification_states),
                "tasks_cache_counts": {uid: len(tasks_cache.get(uid, [])) for uid in list(tasks_cache.keys())[:10]},
                "memory_usage_mb": _get_memory_usage_mb(),
            }
        except Exception as e:
            return {"error": f"failed to collect metrics: {e}"}

    def _forward_metrics():
        if MAIN_LOOP is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(_collect_metrics(), MAIN_LOOP)
                return future.result(timeout=1.0)
            except Exception:
                return {"error": "failed to collect metrics"}
        else:
            return {"error": "bot main loop not available"}

    try:
        register_monitoring(_forward_metrics)
    except Exception:
        pass

    logger.info("✅ Bot initialized!")

async def _graceful_shutdown(application: Application):
    """Graceful shutdown"""
    try:
        await shutdown_cleanup()
    except Exception:
        pass
    try:
        await application.stop()
    except Exception:
        pass

def _get_memory_usage_mb():
    """Get memory usage"""
    try:
        import psutil
        process = psutil.Process()
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except ImportError:
        return None

# =================== MAIN ===================

def main():
    """Main entry point"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return

    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return

    logger.info("🤖 Starting Forwarder Bot...")
    logger.info(f"📊 Loaded {len(USER_SESSIONS)} string sessions from environment")

    start_server_thread()

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Register handlers
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
        # Fallback cleanup
        try:
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
                    pass
            else:
                tmp_loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(tmp_loop)
                    tmp_loop.run_until_complete(shutdown_cleanup())
                finally:
                    try:
                        tmp_loop.close()
                    except Exception:
                        pass
        except Exception:
            pass

if __name__ == "__main__":
    main()
