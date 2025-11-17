import os
import asyncio
from typing import Dict
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from database import Database
from webserver import start_server_thread

BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '0'))

db = Database()
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this bot.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    
    if not db.is_user_allowed(user_id):
        if update.message:
            await update.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        return False
    
    return True

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    user = db.get_user(user_id)
    
    user_name = update.effective_user.first_name or "User"
    user_phone = user['phone'] if user and user['phone'] else "Not connected"
    is_logged_in = user and user['is_logged_in']
    
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
  • /login - Connect your Telegram account
  • /logout - Disconnect your account

📨 **Forwarding Tasks:**
  • /forwadd [LABEL] [SOURCE] => [TARGET]
     Example: /forwadd task1 123456 => 789012
  • /foremove [LABEL] - Remove a task
  • /fortasks - List all your tasks

🆔 **Utilities:**
  • /getallid - Get all your chat IDs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ **How it works:**
1. Connect your account with /login
2. Create a forwarding task
3. Send ONLY NUMBERS in source chat
4. Bot forwards to target (no "Forwarded from" tag!)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
        parse_mode='Markdown'
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

async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    
    if not await check_authorization(update, context):
        return
    
    message = update.message if update.message else update.callback_query.message
    
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    
    login_states[user_id] = {
        'client': client,
        'step': 'waiting_phone'
    }
    
    await message.reply_text(
        "📱 **Enter your phone number** (with country code):\n\n"
        "Example: `+1234567890`\n\n"
        "⚠️ Make sure to include the + sign!",
        parse_mode='Markdown'
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
    client = state['client']
    
    try:
        if state['step'] == 'waiting_phone':
            processing_msg = await update.message.reply_text(
                "⏳ **Processing...**\n\n"
                "Requesting verification code from Telegram...",
                parse_mode='Markdown'
            )
            
            result = await client.send_code_request(text)
            state['phone'] = text
            state['phone_code_hash'] = result.phone_code_hash
            state['step'] = 'waiting_code'
            
            await processing_msg.edit_text(
                "✅ **Code sent!**\n\n"
                "🔑 **Enter the verification code in this format:**\n\n"
                "`verify12345`\n\n"
                "⚠️ Type 'verify' followed immediately by your code (no spaces, no brackets).\n"
                "Example: If your code is 54321, type: `verify54321`",
                parse_mode='Markdown'
            )
        
        elif state['step'] == 'waiting_code':
            if not text.startswith('verify'):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use this format:\n"
                    "`verify12345`\n\n"
                    "Type 'verify' followed immediately by your code.\n"
                    "Example: If your code is 54321, type: `verify54321`",
                    parse_mode='Markdown'
                )
                return
            
            code = text[6:]
            
            if not code or not code.isdigit():
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\n"
                    "Please type 'verify' followed by your verification code.\n"
                    "Example: `verify12345`",
                    parse_mode='Markdown'
                )
                return
            
            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying...**\n\n"
                "Checking your verification code...",
                parse_mode='Markdown'
            )
            
            try:
                await client.sign_in(state['phone'], code, phone_code_hash=state['phone_code_hash'])
                
                me = await client.get_me()
                session_string = client.session.save()
                
                db.save_user(
                    user_id=user_id,
                    phone=state['phone'],
                    name=me.first_name,
                    session_data=session_string,
                    is_logged_in=True
                )
                
                user_clients[user_id] = client
                await start_forwarding_for_user(user_id)
                
                del login_states[user_id]
                
                await verifying_msg.edit_text(
                    "✅ **Successfully connected!**\n\n"
                    f"👤 Name: {me.first_name}\n"
                    f"📱 Phone: {state['phone']}\n\n"
                    "You can now create forwarding tasks with:\n"
                    "`/forwadd [LABEL] [SOURCE_ID] => [TARGET_ID]`",
                    parse_mode='Markdown'
                )
            
            except SessionPasswordNeededError:
                state['step'] = 'waiting_2fa'
                await verifying_msg.edit_text(
                    "🔐 **2FA Password Required**\n\n"
                    "**Enter your 2-step verification password in this format:**\n\n"
                    "`passwordYourPassword123`\n\n"
                    "⚠️ Type 'password' followed immediately by your 2FA password (no spaces, no brackets).\n"
                    "Example: If your password is 'mypass123', type: `passwordmypass123`",
                    parse_mode='Markdown'
                )
        
        elif state['step'] == 'waiting_2fa':
            if not text.startswith('password'):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use this format:\n"
                    "`passwordYourPassword123`\n\n"
                    "Type 'password' followed immediately by your 2FA password.\n"
                    "Example: If your password is 'mypass123', type: `passwordmypass123`",
                    parse_mode='Markdown'
                )
                return
            
            password = text[8:]
            
            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**\n\n"
                    "Please type 'password' followed by your 2FA password.\n"
                    "Example: `passwordmypass123`",
                    parse_mode='Markdown'
                )
                return
            
            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA...**\n\n"
                "Checking your password...",
                parse_mode='Markdown'
            )
            
            await client.sign_in(password=password)
            
            me = await client.get_me()
            session_string = client.session.save()
            
            db.save_user(
                user_id=user_id,
                phone=state['phone'],
                name=me.first_name,
                session_data=session_string,
                is_logged_in=True
            )
            
            user_clients[user_id] = client
            await start_forwarding_for_user(user_id)
            
            del login_states[user_id]
            
            await verifying_msg.edit_text(
                "✅ **Successfully connected!**\n\n"
                f"👤 Name: {me.first_name}\n"
                f"📱 Phone: {state['phone']}\n\n"
                "You can now create forwarding tasks!",
                parse_mode='Markdown'
            )
    
    except Exception as e:
        await update.message.reply_text(
            f"❌ **Error:** {str(e)}\n\n"
            "Please try /login again.",
            parse_mode='Markdown'
        )
        if user_id in login_states:
            del login_states[user_id]

async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    
    if not await check_authorization(update, context):
        return
    
    message = update.message if update.message else update.callback_query.message
    
    user = db.get_user(user_id)
    if not user or not user['is_logged_in']:
        await message.reply_text(
            "❌ **You're not connected!**\n\n"
            "Use /login to connect your account.",
            parse_mode='Markdown'
        )
        return
    
    logout_states[user_id] = {'phone': user['phone']}
    
    await message.reply_text(
        "⚠️ **Confirm Logout**\n\n"
        f"📱 **Enter your phone number to confirm disconnection:**\n\n"
        f"Your connected phone: `{user['phone']}`\n\n"
        "Type your phone number exactly to confirm logout.",
        parse_mode='Markdown'
    )

async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    
    if user_id not in logout_states:
        return False
    
    text = update.message.text.strip()
    stored_phone = logout_states[user_id]['phone']
    
    if text != stored_phone:
        await update.message.reply_text(
            "❌ **Phone number doesn't match!**\n\n"
            f"Expected: `{stored_phone}`\n"
            f"You entered: `{text}`\n\n"
            "Please try again or use /start to cancel.",
            parse_mode='Markdown'
        )
        return True
    
    if user_id in user_clients:
        client = user_clients[user_id]
        await client.disconnect()
        del user_clients[user_id]
    
    db.save_user(user_id, is_logged_in=False)
    del logout_states[user_id]
    
    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n"
        "✅ All your forwarding tasks have been stopped.\n"
        "🔄 Use /login to connect again.",
        parse_mode='Markdown'
    )
    return True

async def forwadd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    user = db.get_user(user_id)
    if not user or not user['is_logged_in']:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\n"
            "Use /login to connect your Telegram account.",
            parse_mode='Markdown'
        )
        return
    
    text = update.message.text.strip()
    
    try:
        parts = text.split(' ', 1)
        if len(parts) < 2 or '=>' not in parts[1]:
            raise ValueError("Invalid format")
        
        label_and_source, target_part = parts[1].split('=>')
        label_parts = label_and_source.strip().split()
        
        if len(label_parts) < 2:
            raise ValueError("Invalid format")
        
        label = label_parts[0]
        source_ids = [int(x) for x in label_parts[1:]]
        target_ids = [int(x.strip()) for x in target_part.split()]
        
        if db.add_forwarding_task(user_id, label, source_ids, target_ids):
            await update.message.reply_text(
                f"✅ **Task created: {label}**\n\n"
                f"📥 Sources: {', '.join(map(str, source_ids))}\n"
                f"📤 Targets: {', '.join(map(str, target_ids))}\n\n"
                "Send number-only messages in the source chats to forward them!",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                f"❌ **Task '{label}' already exists!**\n\n"
                "Use /foremove to delete it first.",
                parse_mode='Markdown'
            )
    
    except Exception as e:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:**\n"
            "`/forwadd [LABEL] [SOURCE_ID] => [TARGET_ID]`\n\n"
            "**Example:**\n"
            "`/forwadd task1 123456789 => 987654321`",
            parse_mode='Markdown'
        )

async def foremove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    text = update.message.text.strip()
    parts = text.split()
    
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:** `/foremove [LABEL]`\n\n"
            "**Example:** `/foremove task1`",
            parse_mode='Markdown'
        )
        return
    
    label = parts[1]
    
    if db.remove_forwarding_task(user_id, label):
        await update.message.reply_text(
            f"✅ **Task '{label}' removed!**",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            f"❌ **Task '{label}' not found!**",
            parse_mode='Markdown'
        )

async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    
    if not await check_authorization(update, context):
        return
    
    message = update.message if update.message else update.callback_query.message
    
    tasks = db.get_user_tasks(user_id)
    
    if not tasks:
        await message.reply_text(
            "📋 **No Active Tasks**\n\n"
            "You don't have any forwarding tasks yet.\n\n"
            "Create one with:\n"
            "`/forwadd [LABEL] [SOURCE_ID] => [TARGET_ID]`",
            parse_mode='Markdown'
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
    
    await message.reply_text(task_list, parse_mode='Markdown')

async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    user = db.get_user(user_id)
    if not user or not user['is_logged_in']:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\n"
            "Use /login to connect.",
            parse_mode='Markdown'
        )
        return
    
    await update.message.reply_text("🔄 **Fetching your chats...**")
    
    await show_chat_categories(user_id, update.message.chat.id, None, context)

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
        [
            InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"),
            InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")
        ],
        [
            InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"),
            InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")
        ]
    ]
    
    if message_id:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

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
            if isinstance(entity, Channel) and entity.broadcast:
                categorized_dialogs.append(dialog)
        elif category == "groups":
            if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and entity.broadcast):
                categorized_dialogs.append(dialog)
        elif category == "private":
            if isinstance(entity, User) and not entity.bot:
                categorized_dialogs.append(dialog)
    
    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]
    
    category_emoji = {
        "bots": "🤖",
        "channels": "📢",
        "groups": "👥",
        "private": "👤"
    }
    
    category_name = {
        "bots": "Bots",
        "channels": "Channels",
        "groups": "Groups",
        "private": "Private Chats"
    }
    
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
    
    await context.bot.edit_message_text(
        chat_list,
        chat_id=chat_id,
        message_id=message_id,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    if not db.is_user_admin(user_id):
        await update.message.reply_text(
            "❌ **Admin Only**\n\n"
            "This command is only available to admins.",
            parse_mode='Markdown'
        )
        return
    
    text = update.message.text.strip()
    parts = text.split()
    
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:**\n"
            "`/adduser [USER_ID]` - Add regular user\n"
            "`/adduser [USER_ID] admin` - Add admin user",
            parse_mode='Markdown'
        )
        return
    
    try:
        new_user_id = int(parts[1])
        is_admin = len(parts) > 2 and parts[2].lower() == 'admin'
        
        if db.add_allowed_user(new_user_id, is_admin=is_admin, added_by=user_id):
            role = "👑 Admin" if is_admin else "👤 User"
            await update.message.reply_text(
                f"✅ **User added!**\n\n"
                f"ID: `{new_user_id}`\n"
                f"Role: {role}",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                f"❌ **User `{new_user_id}` already exists!**",
                parse_mode='Markdown'
            )
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid user ID!**\n\nUser ID must be a number.",
            parse_mode='Markdown'
        )

async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    if not db.is_user_admin(user_id):
        await update.message.reply_text(
            "❌ **Admin Only**\n\n"
            "This command is only available to admins.",
            parse_mode='Markdown'
        )
        return
    
    text = update.message.text.strip()
    parts = text.split()
    
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:** `/removeuser [USER_ID]`",
            parse_mode='Markdown'
        )
        return
    
    try:
        remove_user_id = int(parts[1])
        
        if db.remove_allowed_user(remove_user_id):
            await update.message.reply_text(
                f"✅ **User `{remove_user_id}` removed!**",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                f"❌ **User `{remove_user_id}` not found!**",
                parse_mode='Markdown'
            )
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid user ID!**\n\nUser ID must be a number.",
            parse_mode='Markdown'
        )

async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await check_authorization(update, context):
        return
    
    if not db.is_user_admin(user_id):
        await update.message.reply_text(
            "❌ **Admin Only**\n\n"
            "This command is only available to admins.",
            parse_mode='Markdown'
        )
        return
    
    users = db.get_all_allowed_users()
    
    if not users:
        await update.message.reply_text(
            "📋 **No Allowed Users**\n\n"
            "The allowed users list is empty.",
            parse_mode='Markdown'
        )
        return
    
    user_list = "👥 **Allowed Users**\n\n"
    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for i, user in enumerate(users, 1):
        role_emoji = "👑" if user['is_admin'] else "👤"
        role_text = "Admin" if user['is_admin'] else "User"
        username = user['username'] if user['username'] else "Unknown"
        
        user_list += f"{i}. {role_emoji} **{role_text}**\n"
        user_list += f"   ID: `{user['user_id']}`\n"
        if user['username']:
            user_list += f"   Username: {username}\n"
        user_list += "\n"
    
    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    user_list += f"Total: **{len(users)} user(s)**"
    
    await update.message.reply_text(user_list, parse_mode='Markdown')

async def start_forwarding_for_user(user_id: int):
    if user_id not in user_clients:
        return
    
    client = user_clients[user_id]
    
    @client.on(events.NewMessage())
    async def handle_new_message(event):
        try:
            message_text = event.message.message
            
            if not message_text or not message_text.strip().isdigit():
                return
            
            tasks = db.get_user_tasks(user_id)
            
            for task in tasks:
                if event.chat_id in task['source_ids']:
                    for target_id in task['target_ids']:
                        try:
                            target_entity = None
                            
                            try:
                                target_entity = await client.get_input_entity(int(target_id))
                            except Exception:
                                async for dialog in client.iter_dialogs():
                                    if dialog.id == int(target_id):
                                        target_entity = dialog.entity
                                        break
                            
                            if target_entity:
                                await client.send_message(target_entity, message_text)
                                print(f"✅ [{user_id}] Forwarded '{message_text}' to {target_id}")
                            else:
                                print(f"❌ [{user_id}] Could not resolve entity for {target_id}. Try using /getallid to ensure the chat is in your dialogs.")
                        except Exception as e:
                            print(f"❌ [{user_id}] Error forwarding to {target_id}: {e}")
        
        except Exception as e:
            print(f"❌ Error in message handler for user {user_id}: {e}")

async def restore_sessions():
    print("🔄 Restoring sessions...")
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, session_data FROM users WHERE is_logged_in = 1')
    users = cursor.fetchall()
    conn.close()
    
    print(f"📊 Found {len(users)} logged in user(s)")
    
    for user_id, session_data in users:
        if session_data:
            try:
                client = TelegramClient(
                    StringSession(session_data),
                    API_ID,
                    API_HASH
                )
                await client.connect()
                
                if await client.is_user_authorized():
                    user_clients[user_id] = client
                    await start_forwarding_for_user(user_id)
                    print(f"✅ Restored session for user {user_id}")
                else:
                    db.save_user(user_id, is_logged_in=False)
                    print(f"⚠️ Session expired for user {user_id}")
            except Exception as e:
                print(f"❌ Failed to restore session for user {user_id}: {e}")
                db.save_user(user_id, is_logged_in=False)

async def post_init(application: Application):
    print("🔧 Initializing bot...")
    
    await application.bot.delete_webhook(drop_pending_updates=True)
    print("🧹 Cleared webhooks")
    
    admin_user_id = os.getenv('ADMIN_USER_ID')
    if admin_user_id:
        try:
            admin_id = int(admin_user_id)
            if not db.is_user_allowed(admin_id):
                db.add_allowed_user(admin_id, is_admin=True)
                print(f"✅ Added admin: {admin_id}")
        except ValueError:
            print("⚠️ Invalid ADMIN_USER_ID")
    
    await restore_sessions()
    print("✅ Bot initialized!")

def main():
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN not found")
        return
    
    if not API_ID or not API_HASH:
        print("❌ API_ID or API_HASH not found")
        return
    
    print("🤖 Starting Forwarder Bot...")
    
    start_server_thread()
    
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    
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
    
    print("✅ Bot ready!")
    application.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
