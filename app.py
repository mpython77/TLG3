import asyncio
import json
import os
import threading
import time
import random
from datetime import datetime, timedelta, timezone
import logging
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
from flask_socketio import SocketIO, emit
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError, ChannelPrivateError, UserBannedInChannelError
from telethon.tl.types import PeerChannel, PeerChat, PeerUser
import re
import hashlib

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-here-change-this-in-production')
app.permanent_session_lifetime = timedelta(hours=24)

socketio = SocketIO(app, 
                   cors_allowed_origins="*",
                   logger=False,
                   engineio_logger=False,
                   ping_timeout=60,
                   ping_interval=25,
                   async_mode='threading')

class AuthManager:
    def __init__(self):
        self.password_file = 'password.txt'
        print(f"Looking for password file at: {os.path.abspath(self.password_file)}")
        self.create_default_password_file()
    
    def create_default_password_file(self):
        pass
    
    def load_credentials(self):
        try:
            abs_path = os.path.abspath(self.password_file)
            print(f"Checking password file: {abs_path}")
            print(f"File exists: {os.path.exists(self.password_file)}")
            
            if os.path.exists(self.password_file):
                with open(self.password_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                print(f"Raw file content: {repr(content)}")
                content = content.strip()
                print(f"Stripped content: {repr(content)}")
                
                login = None
                password = None
                
                lines = content.split('\n')
                print(f"Lines: {lines}")
                
                for i, line in enumerate(lines):
                    line = line.strip()
                    print(f"Processing line {i}: {repr(line)}")
                    if line.startswith('Login:'):
                        login = line.replace('Login:', '').strip()
                        print(f"Found login: {repr(login)}")
                    elif line.startswith('Password:'):
                        password = line.replace('Password:', '').strip()
                        print(f"Found password: {repr(password)}")
                
                print(f"Final parsed credentials: login={repr(login)}, password={repr(password)}")
                
                if login and password:
                    return {'login': login, 'password': password}
                else:
                    print("ERROR: Missing login or password in file")
                    return None
            else:
                print(f"ERROR: Password file not found at: {abs_path}")
                return None
        except Exception as e:
            print(f"ERROR loading credentials: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def verify_credentials(self, username, password):
        print(f"\n=== CREDENTIAL VERIFICATION ===")
        print(f"Input username: {repr(username)}")
        print(f"Input password: {repr(password)}")
        
        credentials = self.load_credentials()
        
        if credentials is None:
            print("ERROR: No credentials loaded from file")
            return False
        
        print(f"File username: {repr(credentials['login'])}")
        print(f"File password: {repr(credentials['password'])}")
        
        username_match = username == credentials['login']
        password_match = password == credentials['password']
        
        print(f"Username match: {username_match}")
        print(f"Password match: {password_match}")
        
        result = username_match and password_match
        print(f"Final result: {result}")
        print("=== END VERIFICATION ===\n")
        
        return result

class WebTelegramForwarder:
    def __init__(self):
        self.config_file = 'accounts_config.json'
        self.accounts = self.load_accounts()
        self.clients = {}
        self.running = False
        
        self.min_delay = 15
        self.max_delay = 25
        self.last_forward_time = {}
        
        self.connection_queue = []
        self.current_connecting_phone = None
        self.connection_in_progress = False
        self.connection_paused = False
        
        self.scheduled_posts = []
        self.scheduler_running = False
        self.used_post_ids = set()
        
        self.active_tasks = set()
        self.connection_semaphore = None
        
        self.loop = None
        self.loop_thread = None
        
        self.pending_auth = {}
        
        self.log_history = []
        self.scan_history = []
        self.max_history_size = 500
        
        self.entity_cache = {}
        
        self.scanned_ids = {}
        
        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.WARNING,
            handlers=[
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.start_async_loop()
        
    def start_async_loop(self):
        if self.loop_thread is None or not self.loop_thread.is_alive():
            self.loop_thread = threading.Thread(target=self.run_async_loop, daemon=True)
            self.loop_thread.start()
            
            for i in range(30):
                time.sleep(0.1)
                if self.loop is not None:
                    break
    
    def run_async_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
        
    def load_accounts(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def save_accounts(self):
        try:
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.accounts, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"Error saving accounts: {e}")
    
    def log_message(self, message, account_phone=None):
        utc_plus_1 = timezone(timedelta(hours=2))
        timestamp = datetime.now(utc_plus_1).strftime("%H:%M:%S")
        if account_phone:
            full_message = f"[{timestamp}] [{account_phone}] {message}"
        else:
            full_message = f"[{timestamp}] [SYSTEM] {message}"
        
        self.log_history.append(full_message)
        if len(self.log_history) > self.max_history_size:
            self.log_history = self.log_history[-self.max_history_size:]
        
        try:
            socketio.emit('log_message', {'message': full_message})
        except Exception as e:
            pass
        
        print(full_message)
    
    def scan_message(self, message, account_phone=None, channel=None):
        utc_plus_1 = timezone(timedelta(hours=2))
        timestamp = datetime.now(utc_plus_1).strftime("%H:%M:%S")
        if account_phone and channel:
            full_message = f"[{timestamp}] [{account_phone}] [{channel}] {message}"
        else:
            full_message = f"[{timestamp}] [SCAN] {message}"
        
        self.scan_history.append(full_message)
        if len(self.scan_history) > self.max_history_size:
            self.scan_history = self.scan_history[-self.max_history_size:]
        
        try:
            socketio.emit('scan_message', {'message': full_message})
        except:
            pass
        
        print(full_message)
    
    def get_log_history(self):
        return '\n'.join(self.log_history) if self.log_history else 'System ready... Logs will appear here.'
    
    def get_scan_history(self):
        return '\n'.join(self.scan_history) if self.scan_history else 'Scanner ready... Use "Scan All Post ID" to get message IDs.'
    
    def clear_log_history(self):
        self.log_history = []
        try:
            socketio.emit('clear_logs', {})
        except:
            pass
    
    def clear_scan_history(self):
        self.scan_history = []
        self.scanned_ids = {}
        try:
            socketio.emit('clear_scan', {})
        except:
            pass
    
    def get_scanned_ids(self):
        return self.scanned_ids

    async def get_entity_safe(self, client, entity_id, phone):
        try:
            cache_key = f"{phone}_{entity_id}"
            if cache_key in self.entity_cache:
                return self.entity_cache[cache_key]
            
            original_id = entity_id
            entity_id = int(entity_id)
            
            formats_to_try = []
            
            if entity_id > 0:
                formats_to_try.append(-1000000000000 - entity_id)
                formats_to_try.append(-100000000000 - entity_id)
                formats_to_try.append(-entity_id)
            else:
                formats_to_try.append(entity_id)
                formats_to_try.append(abs(entity_id))
            
            formats_to_try.append(entity_id)
            
            for fmt_id in formats_to_try:
                try:
                    entity = await client.get_entity(fmt_id)
                    self.entity_cache[cache_key] = entity
                    return entity
                except:
                    continue
            
            self.log_message(f"Entity not found with ID {original_id}. Tried formats: {formats_to_try}", phone)
            raise ValueError(f"Could not find entity {original_id}")
                
        except Exception as e:
            self.log_message(f"Error getting entity {entity_id}: {str(e)}", phone)
            raise
    
    def add_account(self, api_id, api_hash, phone, account_name, source_channel, target_channels):
        try:
            int(source_channel)
        except ValueError:
            return {"success": False, "error": "Source channel must be a number (ID)!"}
        
        for account in self.accounts:
            if account['phone'] == phone:
                return {"success": False, "error": "This phone number is already added!"}
        
        if not target_channels:
            return {"success": False, "error": "Add at least one target channel ID!"}
        
        for channel in target_channels:
            try:
                int(channel)
            except ValueError:
                return {"success": False, "error": f"Target channel '{channel}' must be a number (ID)!"}
        
        session_name = f"session_{phone.replace('+', '').replace(' ', '').replace('-', '').replace('(', '').replace(')', '')}"
        session_path = session_name
        
        new_account = {
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone,
            "account_name": account_name or phone,
            "source_channel": source_channel,
            "target_channels": target_channels,
            "status": "Added",
            "session_file": session_path
        }
        
        self.accounts.append(new_account)
        self.save_accounts()
        
        self.log_message(f"New account added: {account_name or phone} ({len(target_channels)} channels)")
        return {"success": True, "message": f"Account added! {len(target_channels)} target channels set."}
    
    def remove_account(self, phone):
        accounts_to_remove = []
        for acc in self.accounts:
            acc_phone = str(acc['phone']).strip()
            if (acc_phone == phone or 
                acc_phone.replace('+', '') == phone.replace('+', '') or
                acc_phone.replace('+', '').replace(' ', '') == phone.replace('+', '').replace(' ', '')):
                accounts_to_remove.append(acc)
        
        if not accounts_to_remove:
            return {"success": False, "error": "Account not found!"}
        
        for acc_to_remove in accounts_to_remove:
            session_file = f"{acc_to_remove['session_file']}.session"
            if os.path.exists(session_file):
                try:
                    os.remove(session_file)
                    self.log_message(f"Session file removed: {session_file}")
                except Exception as e:
                    self.log_message(f"Error removing session file: {str(e)}")
            
            self.accounts.remove(acc_to_remove)
        
        self.save_accounts()
        
        for phone_variant in [phone, phone.replace('+', ''), f"+{phone}"]:
            if phone_variant in self.clients:
                try:
                    if self.loop:
                        asyncio.run_coroutine_threadsafe(self.clients[phone_variant].disconnect(), self.loop)
                    del self.clients[phone_variant]
                    self.log_message(f"Client disconnected: {phone_variant}")
                    break
                except Exception as e:
                    self.log_message(f"Error disconnecting client: {str(e)}")
        
        self.entity_cache.clear()
        
        self.log_message(f"Account removed: {phone}")
        return {"success": True, "message": f"{phone} account removed!"}
    
    def remove_selected_channels(self, selected_channels):
        if not selected_channels:
            return {"success": False, "error": "No channels selected!"}
        
        removed_count = 0
        
        for account in self.accounts:
            phone = account['phone']
            if phone in selected_channels:
                channels_to_remove = selected_channels[phone]
                original_count = len(account['target_channels'])
                
                account['target_channels'] = [ch for ch in account['target_channels'] if ch not in channels_to_remove]
                
                removed_from_this_account = original_count - len(account['target_channels'])
                removed_count += removed_from_this_account
                
                self.log_message(f"Removed {removed_from_this_account} channels from {phone}")
        
        self.save_accounts()
        self.log_message(f"Total channels removed: {removed_count}")
        
        try:
            socketio.emit('accounts_updated', self.get_accounts_data())
        except:
            pass
        
        return {"success": True, "message": f"Removed {removed_count} channels successfully!"}
    
    def get_accounts_data(self):
        accounts_data = []
        for account in self.accounts:
            status = account.get('status', 'Unknown')
            if account['phone'] in self.clients:
                status = 'Connected'
                account['status'] = 'Connected'
            
            accounts_data.append({
                'phone': account['phone'],
                'account_name': account.get('account_name', account['phone']),
                'source_channel': account['source_channel'],
                'target_channels': account.get('target_channels', []),
                'status': status
            })
        
        return {
            'accounts': accounts_data,
            'total_accounts': len(self.accounts),
            'connected_accounts': len(self.clients)
        }
    
    def get_auth_status(self):
        if self.pending_auth:
            for phone, auth_data in self.pending_auth.items():
                return {
                    'auth_required': True,
                    'phone': phone,
                    'step': auth_data['step'],
                    'phone_code_hash': auth_data.get('phone_code_hash', '')
                }
        return {'auth_required': False}
    
    def submit_auth_code(self, phone, code):
        if phone in self.pending_auth and self.pending_auth[phone]['step'] == 'code':
            self.pending_auth[phone]['code'] = code
            return {"success": True, "message": "Code received. Continuing connection..."}
        return {"success": False, "error": "No pending auth for this phone"}
    
    def submit_auth_password(self, phone, password):
        if phone in self.pending_auth and self.pending_auth[phone]['step'] == 'password':
            self.pending_auth[phone]['password'] = password
            return {"success": True, "message": "Password received. Continuing connection..."}
        return {"success": False, "error": "No pending auth for this phone"}
    
    def connect_all_accounts(self):
        if self.connection_in_progress:
            return {"success": False, "error": "Connection already in progress!"}
        
        self.connection_queue = [acc['phone'] for acc in self.accounts if acc['phone'] not in self.clients]
        if not self.connection_queue:
            return {"success": False, "error": "No accounts to connect or all connected!"}
        
        self.connection_in_progress = True
        self.connection_paused = False
        
        threading.Thread(target=self.process_connection_queue, daemon=True).start()
        
        self.log_message(f"Starting connection for {len(self.connection_queue)} accounts")
        return {"success": True, "message": f"Connecting {len(self.connection_queue)} accounts..."}
    
    def process_connection_queue(self):
        while self.connection_queue and self.connection_in_progress and not self.connection_paused:
            self.current_connecting_phone = self.connection_queue.pop(0)
            self.log_message(f"Connecting {self.current_connecting_phone}...")
            
            account = next((acc for acc in self.accounts if acc['phone'] == self.current_connecting_phone), None)
            if not account:
                continue
            
            try:
                client = TelegramClient(
                    account['session_file'],
                    account['api_id'],
                    account['api_hash'],
                    connection_retries=5,
                    retry_delay=5,
                    timeout=15
                )
                
                self.pending_auth[self.current_connecting_phone] = {'step': 'code'}
                phone_code_hash = asyncio.run_coroutine_threadsafe(self.async_sign_in(client, account['phone']), self.loop).result()
                
                self.pending_auth[self.current_connecting_phone]['phone_code_hash'] = phone_code_hash
                
                timeout = 300
                start_time = time.time()
                while time.time() - start_time < timeout:
                    if 'code' in self.pending_auth[self.current_connecting_phone]:
                        code = self.pending_auth[self.current_connecting_phone]['code']
                        del self.pending_auth[self.current_connecting_phone]['code']
                        try:
                            asyncio.run_coroutine_threadsafe(self.async_sign_in_code(client, code, phone_code_hash), self.loop).result()
                            break
                        except SessionPasswordNeededError:
                            self.pending_auth[self.current_connecting_phone]['step'] = 'password'
                            socketio.emit('auth_required', self.get_auth_status())
                            password_timeout = 300
                            password_start = time.time()
                            while time.time() - password_start < password_timeout:
                                if 'password' in self.pending_auth[self.current_connecting_phone]:
                                    password = self.pending_auth[self.current_connecting_phone]['password']
                                    del self.pending_auth[self.current_connecting_phone]['password']
                                    asyncio.run_coroutine_threadsafe(self.async_sign_in_password(client, password), self.loop).result()
                                    break
                                time.sleep(1)
                            if 'password' not in self.pending_auth[self.current_connecting_phone]:
                                break
                        except Exception as e:
                            self.log_message(f"Connection error: {str(e)}", account['phone'])
                            break
                    time.sleep(1)
                
                if self.current_connecting_phone in self.pending_auth:
                    del self.pending_auth[self.current_connecting_phone]
                
                self.clients[account['phone']] = client
                account['status'] = 'Connected'
                self.log_message("Connected successfully", account['phone'])
                
            except Exception as e:
                self.log_message(f"Connection failed: {str(e)}", account['phone'])
            
            socketio.emit('accounts_updated', self.get_accounts_data())
            time.sleep(2)
        
        self.connection_in_progress = False
        self.current_connecting_phone = None
        self.log_message("All connections completed")
    
    async def async_sign_in(self, client, phone):
        await client.connect()
        if not await client.is_user_authorized():
            return await client.send_code_request(phone)
    
    async def async_sign_in_code(self, client, code, phone_code_hash):
        await client.sign_in(client.phone, code, phone_code_hash=phone_code_hash)
    
    async def async_sign_in_password(self, client, password):
        await client.sign_in(password=password)
    
    def scan_all_posts(self):
        self.clear_scan_history()
        self.scanned_ids = {}
        
        if not self.clients:
            return {"success": False, "error": "No connected accounts!"}
        
        threading.Thread(target=self.perform_scan, daemon=True).start()
        
        return {"success": True, "message": "Scanning started..."}
    
    def perform_scan(self):
        for phone, client in self.clients.items():
            account = next(acc for acc in self.accounts if acc['phone'] == phone)
            source_channel = account['source_channel']
            try:
                entity = asyncio.run_coroutine_threadsafe(self.get_entity_safe(client, source_channel, phone), self.loop).result()
                
                messages = asyncio.run_coroutine_threadsafe(client.get_messages(entity, limit=50), self.loop).result()
                
                post_ids = [msg.id for msg in messages if msg.id and not (hasattr(msg, '__class__') and 'MessageService' in str(msg.__class__)) and (msg.text or msg.media)]
                
                self.scanned_ids[phone] = post_ids
                
                self.scan_message(f"Found {len(post_ids)} valid posts", phone, source_channel)
                
            except Exception as e:
                self.scan_message(f"Scan error: {str(e)}", phone, source_channel)
        
        socketio.emit('scan_complete', self.get_scanned_ids())
    
    def add_scheduled_posts(self, post_ids, time_slots, channels):
        if not post_ids:
            return {"success": False, "error": "No post IDs provided!"}
        
        if not time_slots:
            return {"success": False, "error": "No time slots provided!"}
        
        if not channels:
            return {"success": False, "error": "No channels selected!"}
        
        added_count = 0
        post_ids = list(set(post_ids))  # Unique IDs
        
        utc_plus_1 = timezone(timedelta(hours=2))
        
        for time_slot_str in time_slots:
            try:
                dt = datetime.strptime(time_slot_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=utc_plus_1)
            except:
                continue
            
            if dt < datetime.now(utc_plus_1):
                continue
            
            new_post = {
                'id': len(self.scheduled_posts) + 1,
                'datetime': dt,
                'posts': {},
                'status': 'Pending'
            }
            
            total_channels = 0
            
            for phone, ch_list in channels.items():
                if phone not in self.clients:
                    continue
                
                new_post['posts'][phone] = []
                
                for ch in ch_list:
                    if not ch.strip():
                        continue
                    
                    # Fixed: Random selection for each channel independently
                    selected_post_id = random.choice(post_ids)
                    
                    new_post['posts'][phone].append({
                        'channel': ch.strip(),
                        'post_id': selected_post_id
                    })
                    
                    total_channels += 1
            
            if total_channels > 0:
                self.scheduled_posts.append(new_post)
                added_count += 1
                self.log_message(f"Added scheduled post {new_post['id']} for {time_slot_str} - {len(new_post['posts'])} accounts, {total_channels} channels")
        
        self.scheduled_posts.sort(key=lambda x: x['datetime'] if isinstance(x['datetime'], datetime) else datetime.strptime(x['datetime'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=utc_plus_1))
        
        try:
            socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
        except:
            pass
        
        return {"success": True, "message": f"Added {added_count} scheduled posts!"}
    
    def remove_scheduled_post(self, post_id):
        post_to_remove = next((p for p in self.scheduled_posts if p['id'] == post_id), None)
        if post_to_remove:
            if post_to_remove['status'] in ['Sending', 'Sent']:
                return {"success": False, "error": "Cannot remove post that is sending or sent!"}
            
            self.scheduled_posts.remove(post_to_remove)
            self.log_message(f"Removed scheduled post {post_id}")
            
            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass
            
            return {"success": True, "message": "Post removed!"}
        return {"success": False, "error": "Post not found!"}
    
    def get_scheduled_posts_data(self):
        data = []
        for post in self.scheduled_posts:
            total_accounts = len(post['posts'])
            total_channels = sum(len(ch) for ch in post['posts'].values())
            
            all_message_ids = [ch['post_id'] for phone_ch in post['posts'].values() for ch in phone_ch]
            message_ids_str = ', '.join(map(str, sorted(set(all_message_ids)))) if all_message_ids else 'None'
            
            dt = post['datetime'] if isinstance(post['datetime'], datetime) else datetime.strptime(post['datetime'], '%Y-%m-%d %H:%M:%S')
            
            data.append({
                'id': post['id'],
                'datetime': dt.strftime('%d.%m.%Y %H:%M'),
                'message_ids': message_ids_str,
                'distribution': f"{total_accounts} accounts, {total_channels} channels",
                'status': post['status']
            })
        return {'posts': data}
    
    def start_scheduler(self):
        if self.scheduler_running:
            return {"success": False, "error": "Scheduler already running!"}
        
        self.scheduler_running = True
        threading.Thread(target=lambda: asyncio.run_coroutine_threadsafe(self.run_scheduler(), self.loop), daemon=True).start()
        
        try:
            socketio.emit('scheduler_status', {'running': True})
        except:
            pass
        
        self.log_message("Scheduler started")
        return {"success": True, "message": "Scheduler started!"}
    
    async def run_scheduler(self):
        while self.scheduler_running:
            try:
                now = datetime.now(timezone(timedelta(hours=2)))
                
                posts_to_send = [p for p in self.scheduled_posts if p['status'] == 'Pending' and p['datetime'] <= now]
                
                if posts_to_send:
                    posts_to_send.sort(key=lambda x: x['datetime'] if isinstance(x['datetime'], datetime) else datetime.strptime(x['datetime'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone(timedelta(hours=2))))
                    
                    for post in posts_to_send:
                        if self.scheduler_running:
                            self.log_message(f"Sending post {post['id']}")
                            await self.send_scheduled_post(post)
                            if len(posts_to_send) > 1:
                                await asyncio.sleep(30)
                else:
                    pending_count = len([p for p in self.scheduled_posts if p['status'] == 'Pending'])
                    if pending_count > 0:
                        self.log_message(f"No posts ready to send. {pending_count} posts still pending.")
                
                await asyncio.sleep(10)
                
            except Exception as e:
                self.log_message(f"Scheduler error: {str(e)}")
                await asyncio.sleep(10)
        
        self.log_message("Scheduler stopped")
        try:
            socketio.emit('scheduler_status', {'running': False})
        except:
            pass
    
    async def send_scheduled_post(self, post):
        try:
            post['status'] = 'Sending'
            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass
            
            self.log_message(f"Starting to send scheduled post ID {post['id']}")
            
            success_count = 0
            total_count = 0
            
            for phone, channels_data in post['posts'].items():
                if phone not in self.clients:
                    self.log_message(f"Account not connected: {phone}")
                    continue
                
                client = self.clients[phone]
                self.log_message(f"Using account {phone} for {len(channels_data)} channels")
                
                for ch_info in channels_data:
                    channel = ch_info['channel']
                    post_id = ch_info['post_id']
                    total_count += 1
                    
                    try:
                        delay = random.uniform(self.min_delay, self.max_delay)
                        self.log_message(f"Waiting {delay:.1f} seconds before sending to channel {channel}", phone)
                        await asyncio.sleep(delay)
                        
                        await self.send_single_scheduled_post(client, post_id, channel, phone)
                        success_count += 1
                        self.log_message(f"✓ Successfully sent post {post_id} to channel {channel}", phone)
                        
                    except FloodWaitError as e:
                        self.log_message(f"✗ FloodWait {e.seconds}s for channel {channel} - Try increasing delay", phone)
                    except ChannelPrivateError:
                        self.log_message(f"✗ Channel {channel} is private or bot not member", phone)
                    except UserBannedInChannelError:
                        self.log_message(f"✗ User banned in channel {channel}", phone)
                    except Exception as e:
                        error_type = type(e).__name__
                        self.log_message(f"✗ Failed channel {channel}: {error_type} - {str(e)}", phone)
            
            if success_count == total_count and total_count > 0:
                post['status'] = 'Sent'
                self.log_message(f"Post {post['id']} fully sent: {success_count}/{total_count} successful")
            elif success_count > 0:
                post['status'] = f'Partial ({success_count}/{total_count})'
                self.log_message(f"Post {post['id']} partially sent: {success_count}/{total_count} successful")
            else:
                post['status'] = 'Error'
                self.log_message(f"Post {post['id']} failed: 0/{total_count} successful")
            
            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass
            
        except Exception as e:
            post['status'] = 'Error'
            self.log_message(f"Scheduled post general error: {str(e)}")
            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass
    
    async def send_single_scheduled_post(self, client, post_input, target_channel, phone):
        try:
            account = next((acc for acc in self.accounts if acc['phone'] == phone), None)
            if not account:
                raise ValueError("Account not found")
            
            source_channel_id = int(account['source_channel'])
            message_id = int(post_input)
            target_channel_id = int(target_channel)
            
            try:
                source_entity = await self.get_entity_safe(client, source_channel_id, phone)
            except Exception as e:
                raise ValueError(f"Source channel {source_channel_id} not accessible: {str(e)}")
            
            try:
                target_entity = await self.get_entity_safe(client, target_channel_id, phone)
            except Exception as e:
                raise ValueError(f"Target channel {target_channel_id} not accessible: {str(e)}")
            
            try:
                message = await client.get_messages(source_entity, ids=message_id)
                if not message:
                    raise ValueError(f"Message {message_id} not found in source channel")
                
                if hasattr(message, '__class__') and 'MessageService' in str(message.__class__):
                    raise ValueError(f"Message {message_id} is a service message and cannot be forwarded")
                
                if not message.text and not message.media:
                    raise ValueError(f"Message {message_id} is empty or service message")
                    
            except Exception as e:
                raise ValueError(f"Cannot get message {message_id}: {str(e)}")
            
            try:
                await client.forward_messages(
                    target_entity, 
                    message, 
                    from_peer=source_entity,
                    drop_author=True,
                    silent=True
                )
            except Exception as e:
                raise ValueError(f"Forward failed: {str(e)}")
            
        except FloodWaitError as e:
            raise
        except ChannelPrivateError:
            raise
        except UserBannedInChannelError:
            raise
        except Exception as e:
            raise
    
    def disconnect_all(self):
        self.running = False
        self.scheduler_running = False
        self.connection_in_progress = False
        self.connection_paused = False
        
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.async_disconnect_all(), self.loop)
        
        self.log_message("Disconnecting all connections...")
        
        try:
            socketio.emit('scheduler_status', {'running': False})
        except:
            pass
        
        return {"success": True, "message": "Disconnecting all accounts..."}
    
    async def async_disconnect_all(self):
        disconnect_tasks = []
        
        for phone, client in list(self.clients.items()):
            try:
                task = asyncio.create_task(self.safe_disconnect_client(client, phone))
                disconnect_tasks.append(task)
            except Exception as e:
                self.log_message(f"Error creating disconnect task: {str(e)}", phone)
        
        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        
        self.clients.clear()
        self.pending_auth.clear()
        self.entity_cache.clear()
        
        try:
            socketio.emit('accounts_updated', self.get_accounts_data())
        except:
            pass
        self.log_message("All connections disconnected")
    
    async def safe_disconnect_client(self, client, phone):
        try:
            await asyncio.wait_for(client.disconnect(), timeout=5.0)
            self.log_message("Connection disconnected", phone)
        except asyncio.TimeoutError:
            self.log_message("Disconnect timeout - forcing close", phone)
        except Exception as e:
            self.log_message(f"Disconnect error: {str(e)}", phone)

auth_manager = AuthManager()
forwarder = WebTelegramForwarder()

def login_required(f):
    def decorated_function(*args, **kwargs):
        if 'authenticated' not in session or not session['authenticated']:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        
        print(f"Login attempt: username='{username}', password='{password}'")
        
        if not username or not password:
            print("Empty username or password")
            return render_template('login.html', error='Please fill in all fields')
        
        if auth_manager.verify_credentials(username, password):
            print("Login successful")
            session['authenticated'] = True
            session['username'] = username
            session.permanent = True
            return redirect(url_for('index'))
        else:
            print("Invalid credentials")
            return render_template('login.html', error='Invalid username or password')
    
    if 'authenticated' in session and session['authenticated']:
        return redirect(url_for('index'))
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/api/server-time')
@login_required
def get_server_time():
    utc_plus_1 = timezone(timedelta(hours=2))
    current_time = datetime.now(utc_plus_1)
    return jsonify({
        'time': current_time.strftime('%H:%M:%S'),
        'date': current_time.strftime('%Y-%m-%d'),
        'formatted_date': current_time.strftime('%d/%m/%Y'),
        'timestamp': current_time.timestamp(),
        'iso': current_time.isoformat()
    })

@app.route('/api/accounts', methods=['GET'])
@login_required
def get_accounts():
    return jsonify(forwarder.get_accounts_data())

@app.route('/api/accounts', methods=['POST'])
@login_required
def add_account():
    data = request.json
    result = forwarder.add_account(
        data['api_id'],
        data['api_hash'],
        data['phone'],
        data.get('account_name', ''),
        data['source_channel'],
        data['target_channels']
    )
    return jsonify(result)

@app.route('/api/accounts/<phone>', methods=['DELETE'])
@login_required
def remove_account(phone):
    result = forwarder.remove_account(phone)
    return jsonify(result)

@app.route('/api/channels/remove', methods=['POST'])
@login_required
def remove_channels():
    data = request.json
    selected_channels = data.get('channels', {})
    result = forwarder.remove_selected_channels(selected_channels)
    return jsonify(result)

@app.route('/api/connect', methods=['POST'])
@login_required
def connect_accounts():
    result = forwarder.connect_all_accounts()
    return jsonify(result)

@app.route('/api/disconnect', methods=['POST'])
@login_required
def disconnect_accounts():
    result = forwarder.disconnect_all()
    return jsonify(result)

@app.route('/api/auth/status', methods=['GET'])
@login_required
def get_auth_status():
    return jsonify(forwarder.get_auth_status())

@app.route('/api/auth/code', methods=['POST'])
@login_required
def submit_auth_code():
    data = request.json
    result = forwarder.submit_auth_code(data['phone'], data['code'])
    return jsonify(result)

@app.route('/api/auth/password', methods=['POST'])
@login_required
def submit_auth_password():
    data = request.json
    result = forwarder.submit_auth_password(data['phone'], data['password'])
    return jsonify(result)

@app.route('/api/scan/posts', methods=['POST'])
@login_required
def scan_posts():
    result = forwarder.scan_all_posts()
    return jsonify(result)

@app.route('/api/scan/ids', methods=['GET'])
@login_required
def get_scanned_ids():
    return jsonify(forwarder.get_scanned_ids())

@app.route('/api/scheduled', methods=['GET'])
@login_required
def get_scheduled_posts():
    return jsonify(forwarder.get_scheduled_posts_data())

@app.route('/api/scheduled', methods=['POST'])
@login_required
def add_scheduled_post():
    data = request.json
    
    result = forwarder.add_scheduled_posts(
        data['post_ids'],
        data['time_slots'],
        data['channels']
    )
    return jsonify(result)

@app.route('/api/scheduled/<int:post_id>', methods=['DELETE'])
@login_required
def remove_scheduled_post(post_id):
    result = forwarder.remove_scheduled_post(post_id)
    return jsonify(result)

@app.route('/api/scheduler/start', methods=['POST'])
@login_required
def start_scheduler():
    result = forwarder.start_scheduler()
    return jsonify(result)

@app.route('/api/logs/clear', methods=['POST'])
@login_required
def clear_logs():
    forwarder.clear_log_history()
    return jsonify({"success": True, "message": "Logs cleared"})

@app.route('/api/scan/clear', methods=['POST'])
@login_required
def clear_scan():
    forwarder.clear_scan_history()
    return jsonify({"success": True, "message": "Scan history cleared"})

@app.route('/api/logs/history', methods=['GET'])
@login_required
def get_log_history():
    return jsonify({"history": forwarder.get_log_history()})

@app.route('/api/scan/history', methods=['GET'])
@login_required
def get_scan_history():
    return jsonify({"history": forwarder.get_scan_history()})

@app.route('/health')
def health():
    return {"status": "healthy", "accounts": len(forwarder.accounts), "connected": len(forwarder.clients)}

@socketio.on('connect')
def handle_connect():
    if 'authenticated' not in session or not session['authenticated']:
        return False
    
    try:
        emit('accounts_updated', forwarder.get_accounts_data())
        emit('scheduled_posts_updated', forwarder.get_scheduled_posts_data())
        emit('scheduler_status', {'running': forwarder.scheduler_running})
        
        emit('log_history', {'history': forwarder.get_log_history()})
        emit('scan_history', {'history': forwarder.get_scan_history()})
        
        print(f"Client connected: {request.sid}")
    except Exception as e:
        print(f"Error in socket connect: {str(e)}")

@socketio.on('disconnect')
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    print("="*50)
    print("STARTING TELEGRAM FORWARDER WITH AUTH")
    print("="*50)
    
    print(f"Current working directory: {os.getcwd()}")
    print(f"Files in directory: {os.listdir('.')}")
    
    auth_test = AuthManager()
    test_creds = auth_test.load_credentials()
    if test_creds:
        print(f"✓ Credentials loaded successfully")
        print(f"  Username: {test_creds['login']}")
        print(f"  Password: {test_creds['password']}")
    else:
        print("✗ Failed to load credentials")
    
    print("="*50)
    
    socketio.run(
        app, 
        host='0.0.0.0', 
        port=port, 
        debug=True,
        allow_unsafe_werkzeug=True,
        logger=False,
        engineio_logger=False
    )
