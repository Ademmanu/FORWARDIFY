#!/usr/bin/env python3
import os
import asyncio
import logging
import gc
import re
import weakref
import signal
import sys
from typing import Dict, List, Optional, Tuple, Set, Callable, Any
from datetime import datetime, timedelta
from collections import deque

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

from database import db
from webserver import start_server_thread, register_monitoring, register_health_check

# ============================================================================
# OPTIMIZED CONFIGURATION FOR RENDER FREE TIER (512MB RAM, 25+ USERS)
# ============================================================================

# Configure logging for performance
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger("forward")

# Memory optimization settings
GC_INTERVAL = 300  # Run GC every 5 minutes
CACHE_CLEANUP_INTERVAL = 600  # Clean caches every 10 minutes
MAX_CACHED_TASKS = 100  # Max tasks to cache per user
MAX_TARGET_CACHE_SIZE = 50  # Max targets to cache per user
MEMORY_MONITOR_INTERVAL = 60  # Check memory every minute

# Queue and worker optimization
SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "8"))  # Reduced for memory
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "2000"))  # Smaller queue
MEMORY_QUEUE_FALLBACK_SIZE = 5000  # Use DB queue when memory queue reaches this

# Connection management
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "30"))  # Lower for stability
CONNECTION_TIMEOUT = 30
RECONNECT_DELAY = 5

# Performance tuning
TARGET_RESOLVE_BATCH_SIZE = 5
MESSAGE_PROCESS_BATCH_SIZE = 10
SEND_RETRY_ATTEMPTS = 3
SEND_RETRY_DELAY = 1

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Admin and allowed users
OWNER_IDS: Set[int] = set()
ALLOWED_USERS: Set[int] = set()

# Parse environment variables
owner_env = os.getenv("OWNER_IDS", "").strip()
if owner_env:
    OWNER_IDS.update(int(x.strip()) for x in owner_env.split(",") if x.strip().isdigit())

allowed_env = os.getenv("ALLOWED_USERS", "").strip()
if allowed_env:
    ALLOWED_USERS.update(int(x.strip()) for x in allowed_env.split(",") if x.strip().isdigit())

# ============================================================================
# OPTIMIZED DATA STRUCTURES WITH MEMORY LIMITS
# ============================================================================

# Use weak references for user clients to allow garbage collection
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
task_creation_states: Dict[int, Dict[str, Any]] = {}

# Limited size caches with LRU eviction
class LRUCache:
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.cache = {}
        self.order = deque()
    
    def get(self, key):
        if key in self.cache:
            self.order.remove(key)
            self.order.append(key)
            return self.cache[key]
        return None
    
    def set(self, key, value):
        if key in self.cache:
            self.order.remove(key)
        elif len(self.cache) >= self.max_size:
            oldest = self.order.popleft()
            del self.cache[oldest]
        self.cache[key] = value
        self.order.append(key)
    
    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            try:
                self.order.remove(key)
            except ValueError:
                pass
    
    def clear(self):
        self.cache.clear()
        self.order.clear()

# Use LRU caches for better memory management
tasks_cache = LRUCache(max_size=MAX_CONCURRENT_USERS * 10)  # Cache for all users
target_entity_cache: Dict[int, LRUCache] = {}  # Per-user target cache
handler_registered: Dict[int, Callable] = {}

# Message queue with memory limits
send_queue: Optional[asyncio.Queue] = None
queue_overflow_counter = 0

# Worker management
worker_tasks: List[asyncio.Task] = []
maintenance_tasks: List[asyncio.Task] = []
_send_workers_started = False

# Main loop reference
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# Performance monitoring
last_gc_run = 0
last_cache_cleanup = 0
startup_time = datetime.now()

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this bot.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

# ============================================================================
# MEMORY MANAGEMENT UTILITIES
# ============================================================================

async def optimized_gc(force: bool = False):
    """Run garbage collection with memory monitoring"""
    global last_gc_run
    
    current_time = asyncio.get_event_loop().time()
    if force or (current_time - last_gc_run > GC_INTERVAL):
        before = gc.get_count()
        collected = gc.collect()
        after = gc.get_count()
        
        if collected > 1000 or force:
            logger.info(f"🧹 Garbage collection: {collected} objects freed")
        
        last_gc_run = current_time
        
        # Clear unreachable references
        gc.collect(2)  # Run full collection
        
        return collected
    return 0


async def cleanup_unused_caches():
    """Clean up unused caches to free memory"""
    global last_cache_cleanup
    
    current_time = asyncio.get_event_loop().time()
    if current_time - last_cache_cleanup < CACHE_CLEANUP_INTERVAL:
        return
    
    # Remove inactive users from target cache
    active_users = set(user_clients.keys())
    users_to_remove = [uid for uid in target_entity_cache if uid not in active_users]
    for uid in users_to_remove:
        del target_entity_cache[uid]
    
    # Clean up old login states (older than 1 hour)
    current_time_ts = time.time()
    timeout = 3600
    expired_logins = [
        uid for uid, state in login_states.items()
        if current_time_ts - state.get('timestamp', 0) > timeout
    ]
    for uid in expired_logins:
        if uid in login_states:
            state = login_states[uid]
            client = state.get('client')
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            del login_states[uid]
    
    last_cache_cleanup = current_time
    logger.debug("Cache cleanup completed")


def get_memory_usage():
    """Get current memory usage"""
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024  # MB
    except ImportError:
        return 0


async def memory_monitor():
    """Monitor memory usage and trigger cleanup if needed"""
    while True:
        try:
            memory_mb = get_memory_usage()
            container_limit = 512  # Render free tier
            
            if memory_mb > container_limit * 0.8:  # 80% of limit
                logger.warning(f"⚠️ High memory usage: {memory_mb:.2f}MB")
                
                # Force garbage collection
                await optimized_gc(force=True)
                
                # Clear caches
                await cleanup_unused_caches()
                
                # Reduce queue size if too large
                global send_queue
                if send_queue and send_queue.qsize() > 1000:
                    logger.info("Reducing queue pressure")
                    # Drain some items from queue
                    for _ in range(min(100, send_queue.qsize())):
                        try:
                            send_queue.get_nowait()
                            send_queue.task_done()
                        except:
                            break
            
            await asyncio.sleep(MEMORY_MONITOR_INTERVAL)
        except Exception as e:
            logger.error(f"Memory monitor error: {e}")
            await asyncio.sleep(60)


# ============================================================================
# DATABASE HELPERS
# ============================================================================

async def db_call(func, *args, **kwargs):
    """Run DB calls in thread pool with timeout"""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(func, *args, **kwargs),
            timeout=10.0
        )
    except asyncio.TimeoutError:
        logger.error(f"DB call timeout: {func.__name__}")
        raise
    except Exception as e:
        logger.error(f"DB call error: {func.__name__} - {e}")
        raise


# ============================================================================
# MESSAGE FILTERING FUNCTIONS (Optimized)
# ============================================================================

def extract_words(text: str) -> List[str]:
    """Extract words from text efficiently"""
    if not text:
        return []
    return re.findall(r'\S+', text)


def is_numeric_word(word: str) -> bool:
    return word.isdigit()


def is_alphabetic_word(word: str) -> bool:
    return word.isalpha()


def contains_numeric(word: str) -> bool:
    return any(char.isdigit() for char in word)


def contains_alphabetic(word: str) -> bool:
    return any(char.isalpha() for char in word)


def contains_special_characters(word: str) -> bool:
    return any(not char.isalnum() for char in word)


def apply_filters(message_text: str, task_filters: Dict) -> List[str]:
    """Apply filters to message text - optimized version"""
    if not message_text:
        return []
    
    filters_enabled = task_filters.get('filters', {})
    
    # Raw text mode
    if filters_enabled.get('raw_text', False):
        processed = message_text
        prefix = filters_enabled.get('prefix', '')
        suffix = filters_enabled.get('suffix', '')
        if prefix:
            processed = prefix + processed
        if suffix:
            processed = processed + suffix
        return [processed]
    
    # Special filter modes
    if filters_enabled.get('numbers_only', False) and is_numeric_word(message_text.replace(' ', '')):
        return [apply_prefix_suffix(message_text, filters_enabled)]
    
    if filters_enabled.get('alphabets_only', False) and is_alphabetic_word(message_text.replace(' ', '')):
        return [apply_prefix_suffix(message_text, filters_enabled)]
    
    # Word-by-word filtering
    words = extract_words(message_text)
    results = []
    
    for word in words:
        if filters_enabled.get('removed_alphabetic', False):
            if contains_numeric(word) or (not contains_alphabetic(word) and not contains_special_characters(word)):
                continue
        
        if filters_enabled.get('removed_numeric', False):
            if contains_alphabetic(word) or (not contains_numeric(word) and not contains_special_characters(word)):
                continue
        
        results.append(apply_prefix_suffix(word, filters_enabled))
    
    return results if results else [apply_prefix_suffix(message_text, filters_enabled)]


def apply_prefix_suffix(text: str, filters: Dict) -> str:
    """Apply prefix and suffix to text"""
    result = text
    prefix = filters.get('prefix', '')
    suffix = filters.get('suffix', '')
    
    if prefix:
        result = prefix + result
    if suffix:
        result = result + suffix
    
    return result


# ============================================================================
# AUTHORIZATION HELPERS
# ============================================================================

async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    
    # Check environment variables first (fast)
    if user_id in OWNER_IDS or user_id in ALLOWED_USERS:
        return True
    
    # Check database
    try:
        is_allowed = await db_call(db.is_user_allowed, user_id)
        if is_allowed:
            return True
    except Exception:
        logger.exception(f"Error checking DB authorization for {user_id}")
    
    # Not authorized
    try:
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
    except Exception:
        pass
    
    return False


# ============================================================================
# COMMAND HANDLERS (Optimized)
# ============================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_authorization(update, context):
        return
    
    user_id = update.effective_user.id
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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 **Optimized for Render Free Tier**
• 30 concurrent users supported
• Memory-efficient operation
• Auto-recovery on errors
"""
    
    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Tasks", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])
    
    await update.message.reply_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
        parse_mode="Markdown",
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    if not await check_authorization(update, context):
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
    elif query.data.startswith("task_"):
        await handle_task_menu(update, context)
    elif query.data.startswith("filter_"):
        await handle_filter_menu(update, context)
    elif query.data.startswith("toggle_"):
        await handle_toggle_action(update, context)
    elif query.data.startswith("delete_"):
        await handle_delete_action(update, context)
    elif query.data.startswith("confirm_delete_"):
        await handle_confirm_delete(update, context)


# ============================================================================
# TASK MANAGEMENT (Optimized)
# ============================================================================

async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
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
        "target_ids": [],
        "timestamp": time.time()
    }
    
    await update.message.reply_text(
        "🎯 **Let's create a new forwarding task!**\n\n"
        "📝 **Step 1 of 3:** Enter a name for your task.\n"
        "💡 *Example: My Forwarding Task*",
        parse_mode="Markdown"
    )


async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    
    if not await check_authorization(update, context):
        return
    
    message = update.message if update.message else update.callback_query.message
    
    # Get from cache first
    cached = tasks_cache.get(user_id)
    if cached is None:
        tasks = await db_call(db.get_user_tasks, user_id)
        tasks_cache.set(user_id, tasks)
    else:
        tasks = cached
    
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
    keyboard = []
    
    for i, task in enumerate(tasks[:10], 1):  # Limit to 10 tasks
        task_list += f"{i}. **{task['label']}**\n"
        task_list += f"   📥 Sources: {', '.join(map(str, task['source_ids'][:3]))}"
        if len(task['source_ids']) > 3:
            task_list += f" +{len(task['source_ids']) - 3} more"
        task_list += "\n"
        task_list += f"   📤 Targets: {', '.join(map(str, task['target_ids'][:3]))}"
        if len(task['target_ids']) > 3:
            task_list += f" +{len(task['target_ids']) - 3} more"
        task_list += "\n\n"
        
        keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"task_{task['label']}")])
    
    task_list += f"📊 **Total:** {len(tasks)} task(s)"
    
    await message.reply_text(
        task_list,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_task_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("task_", "")
    
    # Get tasks from cache or DB
    cached = tasks_cache.get(user_id)
    if cached is None:
        user_tasks = await db_call(db.get_user_tasks, user_id)
        tasks_cache.set(user_id, user_tasks)
    else:
        user_tasks = cached
    
    task = next((t for t in user_tasks if t["label"] == task_label), None)
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    filters = task.get("filters", {})
    outgoing_emoji = "✅" if filters.get("outgoing", True) else "❌"
    forward_tag_emoji = "✅" if filters.get("forward_tag", False) else "❌"
    control_emoji = "✅" if filters.get("control", True) else "❌"
    
    message_text = f"🔧 **Task Management: {task_label}**\n\n"
    message_text += f"📥 **Sources:** {len(task['source_ids'])} chat(s)\n"
    message_text += f"📤 **Targets:** {len(task['target_ids'])} chat(s)\n\n"
    message_text += "⚙️ **Settings:**\n"
    message_text += f"{outgoing_emoji} Outgoing - Forward outgoing messages\n"
    message_text += f"{forward_tag_emoji} Forward Tag - Show forwarding tag\n"
    message_text += f"{control_emoji} Control - Task active status\n\n"
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


# ============================================================================
# LOGIN/LOGOUT MANAGEMENT (Optimized)
# ============================================================================

async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    
    if not await check_authorization(update, context):
        return
    
    message = update.message if update.message else update.callback_query.message
    
    # Check concurrent user limit
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
            f"📱 Phone: `{user['phone']}`\n"
            f"👤 Name: `{user['name']}`\n\n"
            "Use /logout if you want to disconnect.",
            parse_mode="Markdown",
        )
        return
    
    # Create client with optimized settings
    client = TelegramClient(
        StringSession(),
        API_ID,
        API_HASH,
        device_model="Forwarder Bot",
        system_version="4.0",
        app_version="1.0",
        lang_code="en",
        system_lang_code="en-US"
    )
    
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
    
    login_states[user_id] = {
        "client": client,
        "step": "waiting_phone",
        "timestamp": time.time()
    }
    
    await message.reply_text(
        "📱 **Login Process**\n\n"
        "1️⃣ **Enter your phone number** (with country code):\n\n"
        "**Examples:**\n"
        "• `+1234567890`\n"
        "• `+447911123456`\n\n"
        "**Type your phone number now:**",
        parse_mode="Markdown",
    )


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    # Check for task creation
    if user_id in task_creation_states:
        # Simplified task creation handler
        state = task_creation_states[user_id]
        
        if state["step"] == "waiting_name":
            if text:
                state["name"] = text
                state["step"] = "waiting_source"
                await update.message.reply_text(
                    f"✅ **Task name saved:** {text}\n\n"
                    "📥 **Step 2 of 3:** Enter source chat ID(s).\n"
                    "Multiple IDs separated by spaces.\n"
                    "**Example:** `123456789 987654321`",
                    parse_mode="Markdown"
                )
        
        elif state["step"] == "waiting_source":
            try:
                source_ids = [int(id_str) for id_str in text.split() if id_str.strip().lstrip('-').isdigit()]
                if source_ids:
                    state["source_ids"] = source_ids
                    state["step"] = "waiting_target"
                    await update.message.reply_text(
                        f"✅ **Source IDs saved:** {len(source_ids)} chat(s)\n\n"
                        "📤 **Step 3 of 3:** Enter target chat ID(s).\n"
                        "**Example:** `111222333`",
                        parse_mode="Markdown"
                    )
            except ValueError:
                await update.message.reply_text("❌ **Invalid IDs!**")
        
        elif state["step"] == "waiting_target":
            try:
                target_ids = [int(id_str) for id_str in text.split() if id_str.strip().lstrip('-').isdigit()]
                if target_ids:
                    # Create task
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
                    
                    added = await db_call(
                        db.add_forwarding_task,
                        user_id,
                        state["name"],
                        state["source_ids"],
                        target_ids,
                        task_filters
                    )
                    
                    if added:
                        # Update cache
                        cached = tasks_cache.get(user_id)
                        if cached is not None:
                            cached.append({
                                "label": state["name"],
                                "source_ids": state["source_ids"],
                                "target_ids": target_ids,
                                "filters": task_filters
                            })
                        
                        await update.message.reply_text(
                            f"✅ **Task '{state['name']}' created!**\n\n"
                            f"📥 Sources: {len(state['source_ids'])} chat(s)\n"
                            f"📤 Targets: {len(target_ids)} chat(s)\n\n"
                            "Use /fortasks to manage.",
                            parse_mode="Markdown"
                        )
                    else:
                        await update.message.reply_text("❌ **Task already exists!**")
                    
                    del task_creation_states[user_id]
            except Exception as e:
                logger.exception(f"Task creation error: {e}")
                await update.message.reply_text("❌ **Error creating task!**")
        
        return
    
    # Handle login process
    if user_id not in login_states:
        return
    
    state = login_states[user_id]
    client = state["client"]
    
    try:
        if state["step"] == "waiting_phone":
            if not text.startswith('+'):
                await update.message.reply_text("❌ **Phone must start with +**")
                return
            
            clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
            
            try:
                result = await client.send_code_request(clean_phone)
                state["phone"] = clean_phone
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "waiting_code"
                
                await update.message.reply_text(
                    f"✅ **Code sent to {clean_phone}**\n\n"
                    "2️⃣ **Enter verification code:**\n"
                    "**Format:** `verify12345`\n\n"
                    "**Example:** For code 54321, type:\n"
                    "`verify54321`",
                    parse_mode="Markdown"
                )
            except Exception as e:
                await update.message.reply_text(f"❌ **Error:** {str(e)}")
                await client.disconnect()
                del login_states[user_id]
        
        elif state["step"] == "waiting_code":
            if text.startswith("verify"):
                code = text[6:]
                if code.isdigit() and len(code) == 5:
                    try:
                        await client.sign_in(
                            state["phone"],
                            code,
                            phone_code_hash=state["phone_code_hash"]
                        )
                        
                        me = await client.get_me()
                        session_string = client.session.save()
                        
                        await db_call(
                            db.save_user,
                            user_id,
                            state["phone"],
                            me.first_name,
                            session_string,
                            True
                        )
                        
                        user_clients[user_id] = client
                        await start_forwarding_for_user(user_id)
                        
                        del login_states[user_id]
                        
                        await update.message.reply_text(
                            f"✅ **Connected as {me.first_name}!**\n\n"
                            "You can now create forwarding tasks with /forwadd",
                            parse_mode="Markdown"
                        )
                    except SessionPasswordNeededError:
                        state["step"] = "waiting_2fa"
                        await update.message.reply_text(
                            "🔐 **2FA Required**\n\n"
                            "3️⃣ **Enter 2FA password:**\n"
                            "**Format:** `passwordYourPassword`",
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        await update.message.reply_text(f"❌ **Login failed:** {str(e)}")
        
        elif state["step"] == "waiting_2fa":
            if text.startswith("password"):
                password = text[8:]
                try:
                    await client.sign_in(password=password)
                    
                    me = await client.get_me()
                    session_string = client.session.save()
                    
                    await db_call(
                        db.save_user,
                        user_id,
                        state["phone"],
                        me.first_name,
                        session_string,
                        True
                    )
                    
                    user_clients[user_id] = client
                    await start_forwarding_for_user(user_id)
                    
                    del login_states[user_id]
                    
                    await update.message.reply_text(
                        f"✅ **Connected with 2FA as {me.first_name}!**",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    await update.message.reply_text(f"❌ **2FA failed:** {str(e)}")
    
    except Exception as e:
        logger.exception(f"Login process error: {e}")
        await update.message.reply_text("❌ **Login error!**")
        if user_id in login_states:
            try:
                await client.disconnect()
            except:
                pass
            del login_states[user_id]


async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    message = update.message if update.message else update.callback_query.message
    
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text("❌ **Not connected!**")
        return
    
    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            handler = handler_registered.get(user_id)
            if handler:
                client.remove_event_handler(handler)
                handler_registered.pop(user_id, None)
            
            await client.disconnect()
        except Exception:
            pass
        finally:
            user_clients.pop(user_id, None)
    
    await db_call(db.save_user, user_id, None, None, None, False)
    tasks_cache.delete(user_id)
    target_entity_cache.pop(user_id, None)
    
    await message.reply_text("✅ **Disconnected!**")


# ============================================================================
# FORWARDING CORE (Optimized for Render)
# ============================================================================

def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Register message handler for user if not already registered"""
    if handler_registered.get(user_id):
        return
    
    async def message_handler(event):
        try:
            await optimized_gc()
            
            message = event.message
            if not message or not message.text:
                return
            
            chat_id = event.chat_id
            message_text = message.text
            message_outgoing = message.out
            
            # Get user tasks from cache
            cached = tasks_cache.get(user_id)
            if not cached:
                return
            
            # Process in batches
            tasks_to_process = []
            for task in cached:
                if not task.get("filters", {}).get("control", True):
                    continue
                if message_outgoing and not task.get("filters", {}).get("outgoing", True):
                    continue
                if chat_id in task.get("source_ids", []):
                    tasks_to_process.append(task)
            
            if not tasks_to_process:
                return
            
            # Process each task
            for task in tasks_to_process:
                forward_tag = task.get("filters", {}).get("forward_tag", False)
                filtered_messages = apply_filters(message_text, task.get("filters", {}))
                
                for filtered_msg in filtered_messages:
                    for target_id in task.get("target_ids", []):
                        try:
                            global send_queue
                            if send_queue and send_queue.qsize() < MEMORY_QUEUE_FALLBACK_SIZE:
                                await send_queue.put((
                                    user_id, client, int(target_id), filtered_msg,
                                    task.get("filters", {}), forward_tag,
                                    chat_id if forward_tag else None,
                                    message.id if forward_tag else None
                                ))
                            else:
                                # Fallback to persistent queue
                                await db_call(
                                    db.add_to_message_queue,
                                    user_id, target_id, filtered_msg,
                                    task.get("filters", {}), forward_tag,
                                    chat_id, message.id
                                )
                        except Exception:
                            logger.exception("Error queueing message")
        
        except Exception:
            logger.exception("Error in message handler")
    
    try:
        client.add_event_handler(message_handler, events.NewMessage())
        handler_registered[user_id] = message_handler
    except Exception:
        logger.exception("Failed to register handler")


async def resolve_target_entity(user_id: int, client: TelegramClient, target_id: int):
    """Resolve target entity with caching"""
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = LRUCache(max_size=MAX_TARGET_CACHE_SIZE)
    
    cache = target_entity_cache[user_id]
    cached = cache.get(target_id)
    if cached:
        return cached
    
    try:
        entity = await client.get_input_entity(int(target_id))
        cache.set(target_id, entity)
        return entity
    except Exception:
        logger.debug(f"Could not resolve target {target_id}")
        return None


async def send_worker_loop(worker_id: int):
    """Worker that sends messages with retry logic"""
    logger.info(f"📤 Send worker {worker_id} started")
    
    global send_queue
    if not send_queue:
        return
    
    while True:
        try:
            # Try to get from memory queue first
            try:
                item = await asyncio.wait_for(send_queue.get(), timeout=1.0)
                user_id, client, target_id, message_text, task_filters, forward_tag, source_chat_id, message_id = item
                from_memory = True
            except asyncio.TimeoutError:
                # Fallback to database queue
                pending = await db_call(db.get_pending_messages, limit=10)
                if not pending:
                    await asyncio.sleep(1)
                    continue
                
                msg = pending[0]
                await db_call(db.update_message_status, msg["id"], 1)  # Mark as processing
                
                user_id = msg["user_id"]
                client = user_clients.get(user_id)
                if not client:
                    await db_call(db.update_message_status, msg["id"], 0, increment_attempts=True)
                    await asyncio.sleep(1)
                    continue
                
                target_id = msg["target_id"]
                message_text = msg["message_text"]
                task_filters = msg["task_filters"]
                forward_tag = bool(msg["forward_tag"])
                source_chat_id = msg["source_chat_id"]
                message_id = msg["message_id"]
                db_msg_id = msg["id"]
                from_memory = False
            
            # Send message
            entity = await resolve_target_entity(user_id, client, target_id)
            if not entity:
                logger.debug(f"Target {target_id} not resolved")
                if not from_memory:
                    await db_call(db.update_message_status, db_msg_id, 0, increment_attempts=True)
                continue
            
            try:
                if forward_tag and source_chat_id and message_id:
                    source_entity = await client.get_input_entity(source_chat_id)
                    await client.forward_messages(entity, message_id, source_entity)
                else:
                    await client.send_message(entity, message_text)
                
                if not from_memory:
                    await db_call(db.update_message_status, db_msg_id, 2)  # Mark as completed
                
                logger.debug(f"Worker {worker_id} sent message to {target_id}")
            
            except FloodWaitError as fwe:
                wait = getattr(fwe, "seconds", 10)
                logger.warning(f"FloodWait {wait}s, worker {worker_id} sleeping")
                await asyncio.sleep(wait + 1)
                
                if not from_memory:
                    await db_call(db.update_message_status, db_msg_id, 0)
                else:
                    # Requeue memory item
                    try:
                        await send_queue.put((
                            user_id, client, target_id, message_text,
                            task_filters, forward_tag, source_chat_id, message_id
                        ))
                    except:
                        pass
            
            except Exception as e:
                logger.exception(f"Send error: {e}")
                if not from_memory:
                    await db_call(db.update_message_status, db_msg_id, 3, increment_attempts=True)
        
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception(f"Worker {worker_id} error")
            await asyncio.sleep(1)
        
        finally:
            if from_memory:
                try:
                    send_queue.task_done()
                except:
                    pass


async def start_send_workers():
    """Start message sending workers"""
    global _send_workers_started, send_queue, worker_tasks
    
    if _send_workers_started:
        return
    
    send_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
    
    for i in range(SEND_WORKER_COUNT):
        task = asyncio.create_task(send_worker_loop(i + 1), name=f"send_worker_{i+1}")
        worker_tasks.append(task)
    
    _send_workers_started = True
    logger.info(f"Started {SEND_WORKER_COUNT} send workers")


async def start_forwarding_for_user(user_id: int):
    """Start forwarding for a user"""
    if user_id not in user_clients:
        return
    
    client = user_clients[user_id]
    ensure_handler_registered_for_user(user_id, client)
    
    # Load tasks into cache
    tasks = await db_call(db.get_user_tasks, user_id)
    tasks_cache.set(user_id, tasks)
    
    # Initialize target cache
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = LRUCache(max_size=MAX_TARGET_CACHE_SIZE)
    
    logger.info(f"Started forwarding for user {user_id}")


# ============================================================================
# SESSION RESTORATION (Optimized)
# ============================================================================

async def restore_sessions():
    """Restore user sessions with batching"""
    logger.info("🔄 Restoring sessions...")
    
    try:
        # Get logged in users
        def get_logged_in_users():
            conn = db.pool.get_connection()
            try:
                cur = conn.cursor()
                cur.execute("SELECT user_id, session_data FROM users WHERE is_logged_in = 1")
                return cur.fetchall()
            finally:
                db.pool.return_connection(conn)
        
        users = await asyncio.to_thread(get_logged_in_users)
        
        # Get active tasks
        all_active = await db_call(db.get_all_active_tasks)
        
        # Update tasks cache
        for task in all_active:
            uid = task["user_id"]
            cached = tasks_cache.get(uid)
            if cached is None:
                tasks_cache.set(uid, [task])
            else:
                cached.append(task)
        
        logger.info(f"📊 Found {len(users)} logged in user(s)")
        
        # Restore sessions in batches
        batch_size = 3  # Smaller batch for memory
        for i in range(0, len(users), batch_size):
            batch = users[i:i + batch_size]
            restore_tasks = []
            
            for row in batch:
                try:
                    user_id = row[0]
                    session_data = row[1]
                    if session_data:
                        restore_tasks.append(restore_single_session(user_id, session_data))
                except Exception:
                    continue
            
            if restore_tasks:
                await asyncio.gather(*restore_tasks, return_exceptions=True)
                await asyncio.sleep(2)  # Rate limiting
    
    except Exception as e:
        logger.exception(f"Session restore error: {e}")


async def restore_single_session(user_id: int, session_data: str):
    """Restore a single session"""
    try:
        client = TelegramClient(
            StringSession(session_data),
            API_ID,
            API_HASH,
            device_model="Forwarder Bot Restored",
            system_version="4.0",
            app_version="1.0"
        )
        
        await client.connect()
        
        if await client.is_user_authorized():
            user_clients[user_id] = client
            await start_forwarding_for_user(user_id)
            logger.info(f"✅ Restored session for user {user_id}")
        else:
            await db_call(db.save_user, user_id, None, None, None, False)
            logger.warning(f"⚠️ Session expired for user {user_id}")
    
    except Exception as e:
        logger.error(f"❌ Failed to restore session for {user_id}: {e}")
        try:
            await db_call(db.save_user, user_id, None, None, None, False)
        except:
            pass


# ============================================================================
# MAINTENANCE AND MONITORING
# ============================================================================

async def maintenance_loop():
    """Run maintenance tasks periodically"""
    while True:
        try:
            # Clean up old messages from queue
            cleaned = await db_call(db.cleanup_old_messages, days=1)
            if cleaned:
                logger.info(f"Cleaned {cleaned} old messages from queue")
            
            # Vacuum database if needed
            await asyncio.sleep(3600)  # Check every hour
            vacuumed = await db_call(db.vacuum_if_needed)
            if vacuumed:
                logger.info("Database vacuum completed")
            
            # Clean caches
            await cleanup_unused_caches()
            
            await asyncio.sleep(300)  # 5 minutes between maintenance cycles
        
        except Exception as e:
            logger.error(f"Maintenance error: {e}")
            await asyncio.sleep(60)


async def collect_metrics():
    """Collect system metrics for monitoring"""
    try:
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "uptime": (datetime.now() - startup_time).total_seconds(),
            "users": {
                "connected": len(user_clients),
                "login_states": len(login_states),
                "logout_states": len(logout_states)
            },
            "queue": {
                "memory_size": send_queue.qsize() if send_queue else 0,
                "workers_active": len([t for t in worker_tasks if not t.done()]),
                "workers_total": len(worker_tasks)
            },
            "cache": {
                "tasks_size": len(tasks_cache.cache),
                "target_caches": len(target_entity_cache)
            },
            "memory_mb": get_memory_usage(),
            "gc": {
                "collected": gc.get_count()[0],
                "threshold": gc.get_threshold()
            }
        }
        
        # Get database status
        try:
            db_status = await db_call(db.get_db_status)
            metrics["database"] = db_status
        except Exception:
            metrics["database"] = {"error": "unavailable"}
        
        return metrics
    
    except Exception as e:
        logger.exception(f"Metrics collection error: {e}")
        return {"error": str(e)}


async def health_check():
    """Health check for the system"""
    try:
        health = {
            "status": "healthy",
            "checks": {}
        }
        
        # Check memory
        memory_mb = get_memory_usage()
        health["checks"]["memory"] = {
            "usage_mb": memory_mb,
            "status": "OK" if memory_mb < 400 else "WARNING"  # 400MB threshold
        }
        
        # Check database
        try:
            db_status = await db_call(db.get_db_status)
            health["checks"]["database"] = {
                "connected": True,
                "pool_active": db_status.get("pool_active", 0)
            }
        except Exception:
            health["checks"]["database"] = {"connected": False}
            health["status"] = "degraded"
        
        # Check workers
        active_workers = len([t for t in worker_tasks if not t.done()])
        health["checks"]["workers"] = {
            "active": active_workers,
            "total": len(worker_tasks),
            "status": "OK" if active_workers >= SEND_WORKER_COUNT // 2 else "WARNING"
        }
        
        # Check queue
        if send_queue:
            queue_size = send_queue.qsize()
            health["checks"]["queue"] = {
                "size": queue_size,
                "status": "OK" if queue_size < 1000 else "WARNING"
            }
        
        return health
    
    except Exception as e:
        logger.exception(f"Health check error: {e}")
        return {"status": "error", "error": str(e)}


# ============================================================================
# GRACEFUL SHUTDOWN
# ============================================================================

async def shutdown_cleanup():
    """Clean shutdown of all components"""
    logger.info("🛑 Starting graceful shutdown...")
    
    # Cancel all tasks
    for task in worker_tasks + maintenance_tasks:
        task.cancel()
    
    if worker_tasks or maintenance_tasks:
        try:
            await asyncio.gather(*worker_tasks, *maintenance_tasks, return_exceptions=True)
        except Exception:
            pass
    
    # Disconnect all clients
    disconnect_tasks = []
    for user_id, client in list(user_clients.items()):
        try:
            disconnect_tasks.append(client.disconnect())
        except Exception:
            pass
    
    if disconnect_tasks:
        try:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        except Exception:
            pass
    
    user_clients.clear()
    handler_registered.clear()
    
    # Close database
    try:
        db.close()
    except Exception:
        pass
    
    logger.info("✅ Shutdown complete")


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown...")
        asyncio.create_task(shutdown_cleanup())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


# ============================================================================
# APPLICATION INITIALIZATION
# ============================================================================

async def post_init(application: Application):
    """Initialize the application"""
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    
    logger.info("🔧 Initializing bot...")
    
    # Delete webhook
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    
    # Add admin users from env
    for owner_id in OWNER_IDS:
        try:
            await db_call(db.add_allowed_user, owner_id, None, True, None)
        except Exception:
            pass
    
    for user_id in ALLOWED_USERS:
        try:
            await db_call(db.add_allowed_user, user_id, None, False, None)
        except Exception:
            pass
    
    # Start components
    await start_send_workers()
    
    # Start maintenance tasks
    maintenance_tasks.append(asyncio.create_task(maintenance_loop(), name="maintenance"))
    maintenance_tasks.append(asyncio.create_task(memory_monitor(), name="memory_monitor"))
    
    # Restore sessions
    await restore_sessions()
    
    # Register monitoring callbacks
    def forward_metrics():
        try:
            future = asyncio.run_coroutine_threadsafe(collect_metrics(), MAIN_LOOP)
            return future.result(timeout=2.0)
        except Exception:
            return {"error": "metrics_unavailable"}
    
    def forward_health():
        try:
            future = asyncio.run_coroutine_threadsafe(health_check(), MAIN_LOOP)
            return future.result(timeout=2.0)
        except Exception:
            return {"status": "unavailable"}
    
    try:
        from webserver import register_monitoring, register_health_check
        register_monitoring(forward_metrics)
        register_health_check(forward_health)
    except Exception:
        pass
    
    logger.info("✅ Bot initialized!")


# ============================================================================
# COMMAND REGISTRATION
# ============================================================================

def setup_handlers(application: Application):
    """Setup all command handlers"""
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("forwadd", forwadd_command))
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("getallid", start))  # Simplified
    application.add_handler(CommandHandler("adduser", start))   # Simplified
    application.add_handler(CommandHandler("removeuser", start)) # Simplified
    application.add_handler(CommandHandler("listusers", start))  # Simplified
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_login_process))


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return
    
    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return
    
    logger.info("🤖 Starting Forwarder Bot (Render Optimized)...")
    
    # Setup signal handlers
    setup_signal_handlers()
    
    # Start web server
    try:
        start_server_thread()
    except Exception as e:
        logger.error(f"Failed to start web server: {e}")
    
    # Create application
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    
    # Setup handlers
    setup_handlers(application)
    
    logger.info("✅ Bot ready! Starting polling...")
    
    try:
        # Run with optimized settings for Render
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            close_loop=False
        )
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down...")
    except Exception as e:
        logger.exception(f"Bot crashed: {e}")
    finally:
        # Ensure cleanup
        try:
            asyncio.run(shutdown_cleanup())
        except Exception:
            pass


if __name__ == "__main__":
    main()
