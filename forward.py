#!/usr/bin/env python3
import os
import asyncio
import logging
import functools
import time
from typing import Dict, List, Optional, Tuple, Set, Callable
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

# Optimized logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger("forward")

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Memory optimization for 20 concurrent users
SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "6"))
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "2000"))
TARGET_RESOLVE_RETRY_SECONDS = int(os.getenv("TARGET_RESOLVE_RETRY_SECONDS", "20"))

# Rate limiting for 200 messages/minute per user
MAX_MESSAGES_PER_USER_PER_DAY = 50000
MESSAGE_RATE_LIMIT_PER_MINUTE = 200
MAX_CONCURRENT_USERS = 20

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

db = Database()
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}

# Optimized caches with strict limits
MAX_CACHED_TASKS_PER_USER = 10
MAX_CACHED_ENTITIES_PER_USER = 20

tasks_cache: Dict[int, List[Dict]] = {}
target_entity_cache: Dict[int, Dict[int, object]] = {}
handler_registered: Dict[int, Callable] = {}

# Rate limiting tracking with 200/minute capacity
user_message_counts: Dict[int, Dict[str, int]] = {}
user_rate_limits: Dict[int, Dict[str, float]] = {}

send_queue: Optional[asyncio.Queue[Tuple[int, TelegramClient, int, str]]] = None

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this bot.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

worker_tasks: List[asyncio.Task] = []
_send_workers_started = False
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# User session management
user_last_activity: Dict[int, float] = {}
MAX_USER_INACTIVITY_SECONDS = 3600


async def db_call(func, *args, **kwargs):
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))


# ---------- Enhanced Rate Limiting for 200/min ----------
async def check_rate_limit(user_id: int) -> bool:
    """Check if user is within daily and minute rate limits (200/minute)"""
    now = time.time()
    
    # Initialize or reset daily counter
    if user_id not in user_message_counts:
        user_message_counts[user_id] = {"daily_count": 0, "last_reset": now}
    
    user_stats = user_message_counts[user_id]
    
    # Reset daily counter if more than 24 hours passed
    if now - user_stats["last_reset"] >= 86400:
        user_stats["daily_count"] = 0
        user_stats["last_reset"] = now
    
    # Check daily limit
    if user_stats["daily_count"] >= MAX_MESSAGES_PER_USER_PER_DAY:
        logger.warning("User %s exceeded daily message limit", user_id)
        return False
    
    # Enhanced token bucket for 200/minute
    if user_id not in user_rate_limits:
        user_rate_limits[user_id] = {"tokens": MESSAGE_RATE_LIMIT_PER_MINUTE, "last_update": now}
    
    rate_data = user_rate_limits[user_id]
    elapsed = now - rate_data["last_update"]
    
    # Refill tokens based on elapsed time (200 tokens per minute)
    refill_tokens = elapsed * (MESSAGE_RATE_LIMIT_PER_MINUTE / 60)
    rate_data["tokens"] = min(MESSAGE_RATE_LIMIT_PER_MINUTE, rate_data["tokens"] + refill_tokens)
    rate_data["last_update"] = now
    
    # Check if we have at least 1 token
    if rate_data["tokens"] >= 1:
        rate_data["tokens"] -= 1
        return True
    else:
        logger.warning("User %s exceeded rate limit (200/min), tokens: %.2f", user_id, rate_data["tokens"])
        return False


def increment_message_count(user_id: int):
    """Increment daily message count and update activity"""
    if user_id in user_message_counts:
        user_message_counts[user_id]["daily_count"] += 1
    user_last_activity[user_id] = time.time()


# ---------- Strict User Session Management ----------
def can_accept_new_user() -> bool:
    """Check if we can accept a new concurrent user"""
    active_users = len([uid for uid in user_clients.keys() if is_user_active(uid)])
    return active_users < MAX_CONCURRENT_USERS


def is_user_active(user_id: int) -> bool:
    """Check if user is considered active (recent activity)"""
    last_active = user_last_activity.get(user_id, 0)
    return (time.time() - last_active) < MAX_USER_INACTIVITY_SECONDS


async def enforce_user_limits():
    """Enforce concurrent user limits by disconnecting inactive users"""
    if can_accept_new_user():
        return True
        
    # Find least active user to disconnect
    active_users = [(uid, user_last_activity.get(uid, 0)) for uid in user_clients.keys()]
    active_users.sort(key=lambda x: x[1])
    
    for user_id, last_active in active_users:
        if not is_user_active(user_id):
            await disconnect_inactive_user(user_id)
            if can_accept_new_user():
                return True
            break
    
    return can_accept_new_user()


async def disconnect_inactive_user(user_id: int):
    """Disconnect an inactive user to free up slots"""
    logger.info("Disconnecting inactive user %s", user_id)
    
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
            logger.exception("Error disconnecting inactive user %s", user_id)
        finally:
            user_clients.pop(user_id, None)

    # Clear caches
    tasks_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    user_message_counts.pop(user_id, None)
    user_rate_limits.pop(user_id, None)
    user_last_activity.pop(user_id, None)
    
    # Mark as logged out in DB
    try:
        await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        logger.exception("Error saving user logout state for inactive user %s", user_id)


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


# ---------- Memory Optimized Cache Management ----------
def trim_user_cache(user_id: int):
    """Trim cache sizes to prevent memory bloat with strict limits"""
    if user_id in tasks_cache and len(tasks_cache[user_id]) > MAX_CACHED_TASKS_PER_USER:
        tasks_cache[user_id] = tasks_cache[user_id][-MAX_CACHED_TASKS_PER_USER:]
    
    if user_id in target_entity_cache and len(target_entity_cache[user_id]) > MAX_CACHED_ENTITIES_PER_USER:
        items = list(target_entity_cache[user_id].items())
        target_entity_cache[user_id] = dict(items[-MAX_CACHED_ENTITIES_PER_USER:])


def cleanup_inactive_user_caches():
    """Periodically clean up caches for inactive users"""
    current_time = time.time()
    
    # Clean user clients and caches for inactive users
    users_to_remove = []
    for user_id in list(user_clients.keys()):
        if not is_user_active(user_id):
            users_to_remove.append(user_id)
    
    for user_id in users_to_remove:
        asyncio.create_task(disconnect_inactive_user(user_id))
    
    # Clean other caches
    active_user_ids = set(user_clients.keys())
    
    inactive_users = set(tasks_cache.keys()) - active_user_ids
    for user_id in inactive_users:
        tasks_cache.pop(user_id, None)
    
    inactive_users = set(target_entity_cache.keys()) - active_user_ids
    for user_id in inactive_users:
        target_entity_cache.pop(user_id, None)
    
    all_user_ids = set(tasks_cache.keys()) | set(user_clients.keys())
    inactive_users = set(user_message_counts.keys()) - all_user_ids
    for user_id in inactive_users:
        user_message_counts.pop(user_id, None)
        user_rate_limits.pop(user_id, None)
    
    inactive_users = set(user_last_activity.keys()) - all_user_ids
    for user_id in inactive_users:
        user_last_activity.pop(user_id, None)


# ---------- UI handlers (optimized for 20 users) ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    # Update user activity
    user_last_activity[user_id] = time.time()

    user = await db_call(db.get_user, user_id)

    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]

    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"

    # Daily message count info
    daily_count = user_message_counts.get(user_id, {}).get("daily_count", 0)
    remaining = max(0, MAX_MESSAGES_PER_USER_PER_DAY - daily_count)

    # Concurrent users info
    active_users = len([uid for uid in user_clients.keys() if is_user_active(uid)])

    message_text = f"""
╔═══════════════════════════╗
║   📨 FORWARDER BOT 📨   ║
╚═══════════════════════════╝

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}
👥 **Active Users:** {active_users}/{MAX_CONCURRENT_USERS}
📊 **Daily Usage:** {daily_count}/{MAX_MESSAGES_PER_USER_PER_DAY}
🎯 **Remaining:** {remaining} messages
⚡ **Rate Limit:** 200 messages/minute

📋 **COMMANDS:**
🔐 /login - Connect Telegram account
🔓 /logout - Disconnect account
📨 /forwadd [LABEL] [SOURCE] => [TARGET]
🗑️ /foremove [LABEL] - Remove task
📋 /fortasks - List tasks
🆔 /getallid - Get chat IDs
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
    elif query.data.startswith("chatids_"):
        user_id = query.from_user.id
        if query.data == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        else:
            parts = query.data.split("_")
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)


# ---------- Enhanced Login with User Limits ----------
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    # Check user limits
    if not can_accept_new_user():
        if not await enforce_user_limits():
            message = update.message if update.message else update.callback_query.message
            await message.reply_text(
                "❌ **Server at capacity!**\n\n"
                f"Currently {MAX_CONCURRENT_USERS} active users. Please try again later.",
                parse_mode="Markdown",
            )
            return

    message = update.message if update.message else update.callback_query.message

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    login_states[user_id] = {"client": client, "step": "waiting_phone"}
    user_last_activity[user_id] = time.time()

    await message.reply_text(
        "📱 **Enter your phone number** (with country code):\n\n"
        "Example: `+1234567890`\n\n"
        "⚠️ Make sure to include the + sign!",
        parse_mode="Markdown",
    )


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if user_id not in login_states:
        return

    state = login_states[user_id]
    text = update.message.text.strip()
    client = state["client"]

    try:
        if state["step"] == "waiting_phone":
            processing_msg = await update.message.reply_text(
                "⏳ **Processing...**\n\n"
                "Requesting verification code from Telegram...",
                parse_mode="Markdown",
            )

            result = await client.send_code_request(text)
            state["phone"] = text
            state["phone_code_hash"] = result.phone_code_hash
            state["step"] = "waiting_code"

            await processing_msg.edit_text(
                "✅ **Code sent!**\n\n"
                "🔑 **Enter the verification code in this format:**\n\n"
                "`verify12345`\n\n"
                "⚠️ Type 'verify' followed immediately by your code (no spaces, no brackets).\n"
                "Example: If your code is 54321, type: `verify54321`",
                parse_mode="Markdown",
            )

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use this format:\n"
                    "`verify12345`\n\n"
                    "Type 'verify' followed immediately by your code.\n"
                    "Example: If your code is 54321, type: `verify54321`",
                    parse_mode="Markdown",
                )
                return

            code = text[6:]

            if not code or not code.isdigit():
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\n"
                    "Please type 'verify' followed by your verification code.\n"
                    "Example: `verify12345`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying...**\n\n" "Checking your verification code...",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])

                me = await client.get_me()
                session_string = client.session.save()

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                target_entity_cache.setdefault(user_id, {})
                user_last_activity[user_id] = time.time()
                await start_forwarding_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    "✅ **Successfully connected!**\n\n"
                    f"👤 Name: {me.first_name}\n"
                    f"📱 Phone: {state['phone']}\n\n"
                    "You can now create forwarding tasks with:\n"
                    "`/forwadd [LABEL] [SOURCE_ID] => [TARGET_ID]`",
                    parse_mode="Markdown",
                )

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 **2FA Password Required**\n\n"
                    "**Enter your 2-step verification password in this format:**\n\n"
                    "`passwordYourPassword123`\n\n"
                    "⚠️ Type 'password' followed immediately by your 2FA password (no spaces, no brackets).\n"
                    "Example: If your password is 'mypass123', type: `passwordmypass123`",
                    parse_mode="Markdown",
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use this format:\n"
                    "`passwordYourPassword123`\n\n"
                    "Type 'password' followed immediately by your 2FA password.\n"
                    "Example: If your password is 'mypass123', type: `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**\n\n"
                    "Please type 'password' followed by your 2FA password.\n"
                    "Example: `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA...**\n\n" "Checking your password...", parse_mode="Markdown"
            )

            await client.sign_in(password=password)

            me = await client.get_me()
            session_string = client.session.save()

            await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

            user_clients[user_id] = client
            tasks_cache.setdefault(user_id, [])
            target_entity_cache.setdefault(user_id, {})
            user_last_activity[user_id] = time.time()
            await start_forwarding_for_user(user_id)

            del login_states[user_id]

            await verifying_msg.edit_text(
                "✅ **Successfully connected!**\n\n"
                f"👤 Name: {me.first_name}\n"
                f"📱 Phone: {state['phone']}\n\n"
                "You can now create forwarding tasks!",
                parse_mode="Markdown",
            )

    except Exception as e:
        logger.exception("Error during login process for %s", user_id)
        await update.message.reply_text(
            f"❌ **Error:** {str(e)}\n\n" "Please try /login again.",
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

    # Disconnect client's telethon session (if present)
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

    # mark as logged out in DB
    try:
        await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        logger.exception("Error saving user logout state for %s", user_id)
    # clear caches for this user
    tasks_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    user_message_counts.pop(user_id, None)
    user_rate_limits.pop(user_id, None)
    user_last_activity.pop(user_id, None)
    logout_states.pop(user_id, None)

    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n"
        "✅ All your forwarding tasks have been stopped.\n"
        "🔄 Use /login to connect again.",
        parse_mode="Markdown",
    )
    return True


async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user_last_activity[user_id] = time.time()

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\n" "Use /login to connect your Telegram account.", parse_mode="Markdown"
        )
        return

    text = update.message.text.strip()

    try:
        parts = text.split(" ", 1)
        if len(parts) < 2 or "=>" not in parts[1]:
            raise ValueError("Invalid format")

        label_and_source, target_part = parts[1].split("=>")
        label_parts = label_and_source.strip().split()

        if len(label_parts) < 2:
            raise ValueError("Invalid format")

        label = label_parts[0]
        source_ids = [int(x) for x in label_parts[1:]]
        target_ids = [int(x.strip()) for x in target_part.split()]

        added = await db_call(db.add_forwarding_task, user_id, label, source_ids, target_ids)
        if added:
            # Update in-memory cache immediately (hot path)
            tasks_cache.setdefault(user_id, [])
            tasks_cache[user_id].append(
                {"id": None, "label": label, "source_ids": source_ids, "target_ids": target_ids, "is_active": 1}
            )
            # schedule async resolve of target entities (background)
            try:
                asyncio.create_task(resolve_targets_for_user(user_id, target_ids))
            except Exception:
                logger.exception("Failed to schedule resolve_targets_for_user task")

            await update.message.reply_text(
                f"✅ **Task created: {label}**\n\n"
                f"📥 Sources: {', '.join(map(str, source_ids))}\n"
                f"📤 Targets: {', '.join(map(str, target_ids))}\n\n"
                "Send number-only messages in the source chats to forward them!",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(
                f"❌ **Task '{label}' already exists!**\n\n" "Use /foremove to delete it first.",
                parse_mode="Markdown",
            )

    except Exception as e:
        logger.exception("Error in forwadd: %s", e)
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:**\n"
            "`/forwadd [LABEL] [SOURCE_ID] => [TARGET_ID]`\n\n"
            "**Example:**\n"
            "`/forwadd task1 123456789 => 987654321`",
            parse_mode="Markdown",
        )


async def foremove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user_last_activity[user_id] = time.time()

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n" "**Usage:** `/foremove [LABEL]`\n\n" "**Example:** `/foremove task1`", parse_mode="Markdown"
        )
        return

    label = parts[1]

    deleted = await db_call(db.remove_forwarding_task, user_id, label)
    if deleted:
        # Update in-memory cache
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != label]
        await update.message.reply_text(f"✅ **Task '{label}' removed!**", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ **Task '{label}' not found!**", parse_mode="Markdown")


async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    user_last_activity[user_id] = time.time()

    message = update.message if update.message else update.callback_query.message

    # Use in-memory cache for fast response
    tasks = tasks_cache.get(user_id) or []

    if not tasks:
        await message.reply_text(
            "📋 **No Active Tasks**\n\n" "You don't have any forwarding tasks yet.\n\n" "Create one with:\n" "`/forwadd [LABEL] [SOURCE_ID] => [TARGET_ID]`",
            parse_mode="Markdown",
        )
        return

    task_list = "📋 **Your Forwarding Tasks**\n\n"
    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, task in enumerate(tasks, 1):
        task_list += f"{i}. **{task['label']}**\n"
        task_list += f"   📥 Sources: {', '.join(map(str, task['source_ids']))}\n"
        task_list += f"   📤 Targets: {', '.join(map(str, task['target_ids']))}\n\n"

    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    task_list += f"Total: **{len(tasks)} task(s)**"

    await message.reply_text(task_list, parse_mode="Markdown")


async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user_last_activity[user_id] = time.time()

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

    user_last_activity[user_id] = time.time()

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
            "`/adduser [USER_ID]` - Add regular user\n"
            "`/adduser [USER_ID] admin` - Add admin user",
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
            # notify the added user (best-effort)
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

    user_last_activity[user_id] = time.time()

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
            # Best-effort: disconnect user's Telethon client and clear in-memory state
            if remove_user_id in user_clients:
                try:
                    client = user_clients[remove_user_id]
                    # remove event handler if present
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

            # mark as logged out in users table and clear caches
            try:
                await db_call(db.save_user, remove_user_id, None, None, None, False)
            except Exception:
                logger.exception("Error saving user logged_out state for %s", remove_user_id)

            tasks_cache.pop(remove_user_id, None)
            target_entity_cache.pop(remove_user_id, None)
            handler_registered.pop(remove_user_id, None)
            user_message_counts.pop(remove_user_id, None)
            user_rate_limits.pop(remove_user_id, None)
            user_last_activity.pop(remove_user_id, None)

            await update.message.reply_text(f"✅ **User `{remove_user_id}` removed!**", parse_mode="Markdown")

            # Notify removed user (best-effort)
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

    user_last_activity[user_id] = time.time()

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


# ---------- Forwarding core: handler registration, message handler, send worker, resolver ----------
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Attach a NewMessage handler once per client/user to avoid duplicates and store the handler (so it can be removed)."""
    if handler_registered.get(user_id):
        return

    async def _high_capacity_message_handler(event):
        try:
            # Update user activity
            user_last_activity[user_id] = time.time()

            # Fast path: check if message is numeric
            message_text = getattr(event, "raw_text", None) or getattr(getattr(event, "message", None), "message", None)
            if not message_text or not message_text.strip().isdigit():
                return

            chat_id = getattr(event, "chat_id", None) or getattr(getattr(event, "message", None), "chat_id", None)
            if chat_id is None:
                return

            # Check rate limits before processing (200/minute)
            if not await check_rate_limit(user_id):
                return

            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return

            # Process targets efficiently
            processed_targets = 0
            for task in user_tasks:
                if chat_id in task.get("source_ids", []):
                    for target_id in task.get("target_ids", []):
                        if processed_targets >= 5:  # Limit targets per message
                            break
                        try:
                            if send_queue is not None and not send_queue.full():
                                await send_queue.put((user_id, client, int(target_id), message_text))
                                increment_message_count(user_id)
                                processed_targets += 1
                            else:
                                logger.warning("Send queue full, dropping message for user=%s", user_id)
                        except asyncio.QueueFull:
                            logger.warning("Send queue full, dropping message for user=%s", user_id)
                    
        except Exception:
            logger.exception("Error in high capacity message handler for user %s", user_id)

    try:
        client.add_event_handler(_high_capacity_message_handler, events.NewMessage())
        handler_registered[user_id] = _high_capacity_message_handler
        logger.info("Registered high capacity message handler for user %s", user_id)
    except Exception:
        logger.exception("Failed to add event handler for user %s", user_id)


async def resolve_target_entity_once(user_id: int, client: TelegramClient, target_id: int) -> Optional[object]:
    """Try to resolve a target entity and cache it. Returns entity or None."""
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = {}

    if target_id in target_entity_cache[user_id]:
        return target_entity_cache[user_id][target_id]

    try:
        entity = await client.get_input_entity(int(target_id))
        # Trim cache if needed
        if len(target_entity_cache[user_id]) >= MAX_CACHED_ENTITIES_PER_USER:
            # Remove oldest entry (first item)
            first_key = next(iter(target_entity_cache[user_id]))
            target_entity_cache[user_id].pop(first_key)
        
        target_entity_cache[user_id][target_id] = entity
        return entity
    except Exception:
        logger.debug("Could not resolve target %s for user %s now", target_id, user_id)
        return None


async def resolve_targets_for_user(user_id: int, target_ids: List[int]):
    """Background resolver that attempts to resolve targets for a user."""
    client = user_clients.get(user_id)
    if not client:
        return
    for tid in target_ids[:15]:  # Limit to 15 targets
        for attempt in range(2):  # Reduced attempts
            ent = await resolve_target_entity_once(user_id, client, tid)
            if ent:
                logger.info("Resolved target %s for user %s", tid, user_id)
                break
            await asyncio.sleep(TARGET_RESOLVE_RETRY_SECONDS)


async def send_worker_loop(worker_id: int):
    """Optimized worker for high message throughput"""
    logger.info("High capacity send worker %d started", worker_id)
    global send_queue
    
    if send_queue is None:
        return

    # Smaller batches for better responsiveness
    BATCH_SIZE = 3
    BATCH_TIMEOUT = 0.1  # Shorter timeout for faster processing

    while True:
        try:
            batch_messages = []
            
            # Get multiple messages quickly
            for _ in range(BATCH_SIZE):
                try:
                    item = await asyncio.wait_for(send_queue.get(), timeout=BATCH_TIMEOUT)
                    batch_messages.append(item)
                except asyncio.TimeoutError:
                    break

            if batch_messages:
                await process_message_batch(batch_messages, worker_id)

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Error in high capacity send worker %d", worker_id)
            await asyncio.sleep(0.5)


async def process_message_batch(batch: List[Tuple], worker_id: int):
    """Process a batch of messages efficiently with error handling"""
    for user_id, client, target_id, message_text in batch:
        try:
            # Update user activity
            user_last_activity[user_id] = time.time()

            entity = None
            # Try cache first
            if user_id in target_entity_cache:
                entity = target_entity_cache[user_id].get(target_id)
            
            if not entity:
                entity = await resolve_target_entity_once(user_id, client, target_id)
            
            if not entity:
                logger.debug("Skipping send: target %s unresolved for user %s", target_id, user_id)
                continue

            try:
                await client.send_message(entity, message_text)
                logger.debug("Forwarded message for user %s to %s", user_id, target_id)
            except FloodWaitError as fwe:
                wait = int(getattr(fwe, "seconds", 10))
                logger.warning("FloodWait for %s seconds. Pausing worker %d", wait, worker_id)
                await asyncio.sleep(wait + 1)
                # Requeue with backoff
                try:
                    if send_queue is not None and send_queue.qsize() < SEND_QUEUE_MAXSIZE // 2:
                        await send_queue.put((user_id, client, target_id, message_text))
                except asyncio.QueueFull:
                    logger.warning("Send queue full while re-enqueueing after FloodWait")
            except Exception as e:
                logger.exception("Error sending message for user %s to %s: %s", user_id, target_id, e)

        except Exception:
            logger.exception("Unexpected error processing message in worker %d", worker_id)
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
    logger.info("Spawned %d high capacity send workers", SEND_WORKER_COUNT)


async def start_forwarding_for_user(user_id: int):
    """Ensure client exists, register handler (once), and ensure caches created."""
    if user_id not in user_clients:
        return

    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    target_entity_cache.setdefault(user_id, {})

    ensure_handler_registered_for_user(user_id, client)


# ---------- Optimized Session Restoration for 20 Users ----------
async def restore_sessions():
    """Optimized session restoration with strict user limits"""
    logger.info("🔄 Restoring sessions for up to %d users...", MAX_CONCURRENT_USERS)

    MAX_CONCURRENT_RESTORES = 3  # Further reduced for memory
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_RESTORES)

    async def restore_single_session(user_id, session_data):
        async with semaphore:
            # Check if we've reached user limit
            if not can_accept_new_user():
                logger.info("Skipping user %s - server at capacity", user_id)
                return

            if session_data:
                try:
                    client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
                    await client.connect()

                    if await client.is_user_authorized():
                        user_clients[user_id] = client
                        target_entity_cache.setdefault(user_id, {})
                        user_last_activity[user_id] = time.time()
                        
                        # Load limited tasks (max 10)
                        user_tasks = await db_call(db.get_user_tasks_limited, user_id, MAX_CACHED_TASKS_PER_USER)
                        tasks_cache[user_id] = user_tasks
                        
                        # Resolve limited targets (max 5 tasks initially)
                        all_targets = []
                        for task in user_tasks[:5]:
                            all_targets.extend(task.get("target_ids", []))
                        
                        if all_targets:
                            asyncio.create_task(resolve_targets_for_user(user_id, list(set(all_targets))[:15]))
                        
                        await start_forwarding_for_user(user_id)
                        logger.info("✅ Restored session for user %s", user_id)
                    else:
                        await db_call(db.save_user, user_id, None, None, None, False)
                        logger.warning("⚠️ Session expired for user %s", user_id)
                except Exception as e:
                    logger.exception("❌ Failed to restore session for user %s: %s", user_id, e)
                    try:
                        await db_call(db.save_user, user_id, None, None, None, False)
                    except Exception:
                        pass

    try:
        def _fetch_logged_in_users():
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT user_id, session_data FROM users 
                WHERE is_logged_in = 1 
                ORDER BY updated_at DESC 
                LIMIT ?
            """, (MAX_CONCURRENT_USERS * 2,))
            return cur.fetchall()

        users = await asyncio.to_thread(_fetch_logged_in_users)
        logger.info("📊 Found %d logged in user(s), restoring up to %d", len(users), MAX_CONCURRENT_USERS)

        # Restore sessions with strict limits
        restore_tasks = []
        for row in users:
            try:
                user_id = row["user_id"] if hasattr(row, "keys") else row[0]
                session_data = row["session_data"] if hasattr(row, "keys") else row[1]
                restore_tasks.append(restore_single_session(user_id, session_data))
            except Exception:
                continue

        await asyncio.gather(*restore_tasks, return_exceptions=True)
        
    except Exception:
        logger.exception("Error fetching logged-in users from DB")


# ---------- Enhanced Periodic Cleanup ----------
async def periodic_cleanup():
    """Run periodic cleanup tasks to manage memory and user limits"""
    while True:
        await asyncio.sleep(180)  # Run every 3 minutes for better responsiveness
        try:
            cleanup_inactive_user_caches()
            
            # Log current status
            active_users = len([uid for uid in user_clients.keys() if is_user_active(uid)])
            total_messages = sum(stats["daily_count"] for stats in user_message_counts.values())
            
            logger.info("Cleanup: %d active users, %d total messages today", active_users, total_messages)
            
        except Exception:
            logger.exception("Error during periodic cleanup")


# ---------- Modified post_init ----------
async def post_init(application: Application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    logger.info("🔧 Initializing high capacity bot (%d users, 200/min)...", MAX_CONCURRENT_USERS)

    await application.bot.delete_webhook(drop_pending_updates=True)

    # Add owners and allowed users
    if OWNER_IDS:
        for oid in OWNER_IDS:
            try:
                is_admin = await db_call(db.is_user_admin, oid)
                if not is_admin:
                    await db_call(db.add_allowed_user, oid, None, True, None)
            except Exception:
                logger.exception("Error adding owner/admin %s from env", oid)

    if ALLOWED_USERS:
        for au in ALLOWED_USERS:
            try:
                await db_call(db.add_allowed_user, au, None, False, None)
            except Exception:
                logger.exception("Error adding allowed user %s from env", au)

    # Start optimized components
    await start_send_workers()
    await restore_sessions()
    
    # Start periodic cleanup task
    asyncio.create_task(periodic_cleanup())

    # Enhanced metrics
    async def _collect_metrics():
        try:
            q = send_queue.qsize() if send_queue is not None else None
            active_users = len([uid for uid in user_clients.keys() if is_user_active(uid)])
            total_messages = sum(stats["daily_count"] for stats in user_message_counts.values())
            
            return {
                "send_queue_size": q,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": active_users,
                "max_concurrent_users": MAX_CONCURRENT_USERS,
                "total_messages_today": total_messages,
                "user_message_counts": {uid: data["daily_count"] for uid, data in user_message_counts.items()},
            }
        except Exception as e:
            return {"error": f"failed to collect metrics: {e}"}

    def _forward_metrics():
        global MAIN_LOOP
        if MAIN_LOOP is None:
            return {"error": "bot main loop not available"}
        try:
            future = asyncio.run_coroutine_threadsafe(_collect_metrics(), MAIN_LOOP)
            return future.result(timeout=1.0)
        except Exception as e:
            return {"error": f"failed to collect metrics: {e}"}

    try:
        register_monitoring(_forward_metrics)
    except Exception:
        logger.exception("Failed to register monitoring callback")

    logger.info("✅ High capacity bot initialized! Ready for %d users @ 200msgs/min", MAX_CONCURRENT_USERS)


# ---------- Graceful shutdown cleanup ----------
async def shutdown_cleanup():
    """Enhanced shutdown cleanup"""
    logger.info("High capacity shutdown cleanup...")

    # cancel worker tasks
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

    # disconnect all telethon clients
    for uid, client in list(user_clients.items()):
        try:
            handler = handler_registered.get(uid)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    pass
                handler_registered.pop(uid, None)

            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client %s during shutdown", uid)
    user_clients.clear()

    # clear all caches
    tasks_cache.clear()
    target_entity_cache.clear()
    user_message_counts.clear()
    user_rate_limits.clear()
    user_last_activity.clear()

    try:
        db.close_connection()
    except Exception:
        logger.exception("Error closing DB connection")

    logger.info("High capacity shutdown complete.")


# ---------- Main -----------
def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return

    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return

    logger.info("🤖 Starting High Capacity Forwarder Bot (%d users, 200/min)...", MAX_CONCURRENT_USERS)

    # start webserver thread first
    start_server_thread()

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Add all handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("forwadd", forwadd_command))
    application.add_handler(CommandHandler("foremove", foremove_command))
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_login_process))

    logger.info("✅ High capacity bot ready!")
    try:
        application.run_polling(drop_pending_updates=True)
    finally:
        try:
            asyncio.run(shutdown_cleanup())
        except Exception:
            logger.exception("Error during shutdown cleanup")


if __name__ == "__main__":
    main()
