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
from telethon.sessions import StringSession
import re
import hashlib
from database import get_db, DatabaseManager

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
        # Initialize basic variables first (before logging)
        self.log_history = []
        self.scan_history = []
        self.max_history_size = 500

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

        # Now initialize database (after logging is ready)
        self.db = get_db()
        self.log_message("PostgreSQL database initialized")

        # Migrate from JSON if exists
        self.config_file = 'accounts_config.json'
        if os.path.exists(self.config_file):
            migrated = self.db.migrate_from_json(self.config_file)
            if migrated > 0:
                self.log_message(f"Migrated {migrated} accounts from JSON to PostgreSQL")

        self.accounts = self.load_accounts()

        self.start_async_loop()

        # Load scheduled posts from database
        self.load_scheduled_posts_from_db()
        
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
        """Load accounts from PostgreSQL database"""
        try:
            accounts = self.db.get_all_accounts()
            self.logger.info(f"✅ Loaded {len(accounts)} accounts from database")
            return accounts
        except Exception as e:
            self.logger.error(f"❌ Failed to load accounts from database: {e}")
            return []

    def save_accounts(self):
        """Accounts are saved automatically to database, this method kept for compatibility"""
        pass

    def load_scheduled_posts_from_db(self):
        """Load scheduled posts from database"""
        try:
            posts = self.db.get_all_scheduled_posts()
            self.scheduled_posts = posts
            self.logger.info(f"✅ Loaded {len(posts)} scheduled posts from database")
        except Exception as e:
            self.logger.error(f"❌ Failed to load scheduled posts: {e}")
            self.scheduled_posts = []
    
    def log_message(self, message, account_phone=None):
        utc_plus_2 = timezone(timedelta(hours=2))
        timestamp = datetime.now(utc_plus_2).strftime("%H:%M:%S")
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
        utc_plus_2 = timezone(timedelta(hours=2))
        timestamp = datetime.now(utc_plus_2).strftime("%H:%M:%S")
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

        # Check if account already exists
        existing = self.db.get_account_by_phone(phone)
        if existing:
            return {"success": False, "error": "This phone number is already added!"}

        if not target_channels:
            return {"success": False, "error": "Add at least one target channel ID!"}

        for channel in target_channels:
            try:
                int(channel)
            except ValueError:
                return {"success": False, "error": f"Target channel '{channel}' must be a number (ID)!"}

        session_name = f"session_{phone.replace('+', '').replace(' ', '').replace('-', '').replace('(', '').replace(')', '')}"

        new_account_data = {
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone,
            "account_name": account_name or phone,
            "source_channel": source_channel,
            "target_channels": target_channels,
            "status": "Added",
            "session_file": session_name,
            "session_string": None
        }

        try:
            # Add to database
            added_account = self.db.add_account(new_account_data)
            # Reload accounts list
            self.accounts = self.load_accounts()

            self.log_message(f"New account added: {account_name or phone} ({len(target_channels)} channels)")
            return {"success": True, "message": f"Account added! {len(target_channels)} target channels set."}
        except Exception as e:
            self.logger.error(f"Failed to add account: {str(e)}")
            return {"success": False, "error": f"Database error: {str(e)}"}
    
    def remove_account(self, phone):
        # Normalize phone number
        normalized_phones = [phone, phone.replace('+', ''), f"+{phone}"]

        account_to_remove = None
        for normalized in normalized_phones:
            account_to_remove = self.db.get_account_by_phone(normalized)
            if account_to_remove:
                phone = normalized
                break

        if not account_to_remove:
            return {"success": False, "error": "Account not found!"}

        # Remove session file if exists
        session_file = f"{account_to_remove['session_file']}.session"
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
                self.log_message(f"Session file removed: {session_file}")
            except Exception as e:
                self.log_message(f"Error removing session file: {str(e)}")

        # Disconnect client if connected
        for phone_variant in normalized_phones:
            if phone_variant in self.clients:
                try:
                    if self.loop:
                        asyncio.run_coroutine_threadsafe(self.clients[phone_variant].disconnect(), self.loop)
                    del self.clients[phone_variant]
                    self.log_message(f"Client disconnected: {phone_variant}")
                    break
                except Exception as e:
                    self.log_message(f"Error disconnecting client: {str(e)}")

        # Remove from database
        try:
            self.db.delete_account(phone)
            # Reload accounts list
            self.accounts = self.load_accounts()
            self.entity_cache.clear()

            self.log_message(f"Account removed: {phone}")
            return {"success": True, "message": f"{phone} account removed!"}
        except Exception as e:
            self.logger.error(f"Failed to remove account: {str(e)}")
            return {"success": False, "error": f"Database error: {str(e)}"}
    
    def remove_selected_channels(self, selected_channels):
        if not selected_channels:
            return {"success": False, "error": "No channels selected!"}

        removed_count = 0

        for phone, channels_to_remove in selected_channels.items():
            try:
                account = self.db.get_account_by_phone(phone)
                if not account:
                    continue

                original_count = len(account['target_channels'])
                updated_channels = [ch for ch in account['target_channels'] if ch not in channels_to_remove]

                # Update in database
                self.db.update_account(phone, {'target_channels': updated_channels})

                removed_from_this_account = original_count - len(updated_channels)
                removed_count += removed_from_this_account

                self.log_message(f"Removed {removed_from_this_account} channels from {phone}")
            except Exception as e:
                self.logger.error(f"Failed to remove channels from {phone}: {str(e)}")

        # Reload accounts
        self.accounts = self.load_accounts()
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
                    'step': auth_data['step']
                }
        return {'auth_required': False}
    
    def connect_all_accounts(self):
        if not self.accounts:
            return {"success": False, "error": "No accounts available!"}
        
        if self.connection_in_progress:
            return {"success": False, "error": "Connection process is already in progress!"}
        
        if self.pending_auth:
            return {"success": False, "error": "Please complete authentication for the current account first!"}
        
        if self.loop and not self.loop.is_closed():
            self.connection_queue = [acc for acc in self.accounts]
            self.current_connecting_phone = None
            self.connection_in_progress = True
            self.connection_paused = False
            
            try:
                socketio.emit('connection_progress', {
                    'current': 0,
                    'total': len(self.accounts),
                    'status': 'Starting sequential connection...'
                })
            except:
                pass
            
            asyncio.run_coroutine_threadsafe(self.connect_accounts_sequentially(), self.loop)
            return {"success": True, "message": "Sequential connection started!"}
        else:
            return {"success": False, "error": "Async loop not available!"}
    
    async def connect_accounts_sequentially(self):
        connected_count = 0
        failed_count = 0
        
        self.log_message(f"Starting sequential connection for {len(self.connection_queue)} accounts")
        
        for index, account in enumerate(self.connection_queue):
            if not self.connection_in_progress:
                break
                
            phone = account['phone']
            self.current_connecting_phone = phone
            
            self.log_message(f"Processing account {index + 1}/{len(self.connection_queue)}: {phone}")
            
            try:
                socketio.emit('connection_progress', {
                    'current': index + 1,
                    'total': len(self.connection_queue),
                    'status': f"Connecting {phone}..."
                })
            except:
                pass
            
            try:
                result = await self.connect_single_account_sequential(account)
                
                if result == 'auth_required':
                    self.log_message(f"Authentication required for {phone}. Process paused.")
                    self.connection_paused = True
                    
                    try:
                        socketio.emit('connection_progress', {
                            'current': index + 1,
                            'total': len(self.connection_queue),
                            'status': f"Authentication required for {phone}. Process paused.",
                            'paused': True
                        })
                    except:
                        pass
                    return
                    
                elif result == 'success':
                    connected_count += 1
                    self.log_message(f"Successfully connected: {phone}")
                    
                else:
                    failed_count += 1
                    self.log_message(f"Failed to connect: {phone}")
                    
            except Exception as e:
                failed_count += 1
                self.log_message(f"Connection error for {phone}: {str(e)}")
                account['status'] = 'Error'
            
            try:
                socketio.emit('accounts_updated', self.get_accounts_data())
            except:
                pass
            
            if index < len(self.connection_queue) - 1:
                await asyncio.sleep(2)
        
        self.finish_connection_process(connected_count, failed_count)
    
    def finish_connection_process(self, connected_count, failed_count):
        self.connection_in_progress = False
        self.connection_paused = False
        self.current_connecting_phone = None
        total = len(self.connection_queue)

        self.log_message(f"Connection process completed: {connected_count} connected, {failed_count} failed")

        # Auto-start scheduler if there are pending posts and accounts are connected
        if not self.scheduler_running and self.scheduled_posts and self.clients and self.loop:
            pending_posts = [p for p in self.scheduled_posts if p['status'] == 'Pending']
            if pending_posts:
                self.log_message(f"Auto-starting scheduler: found {len(pending_posts)} pending posts")
                self.scheduler_running = True
                asyncio.run_coroutine_threadsafe(self.run_scheduler(), self.loop)
                try:
                    socketio.emit('scheduler_status', {'running': True})
                except:
                    pass

        try:
            socketio.emit('connection_progress', {
                'current': total,
                'total': total,
                'status': f"Process completed: {connected_count} connected, {failed_count} failed",
                'finished': True
            })

            socketio.emit('accounts_updated', self.get_accounts_data())
        except:
            pass
    
    async def connect_single_account_sequential(self, account):
        phone = account['phone']

        try:
            self.log_message(f"Initiating connection...", phone)
            account['status'] = 'Connecting...'

            if phone in self.clients:
                try:
                    old_client = self.clients[phone]
                    del self.clients[phone]
                    await asyncio.wait_for(old_client.disconnect(), timeout=3.0)
                    await asyncio.sleep(0.5)
                except Exception as cleanup_error:
                    self.log_message(f"Cleanup error (continuing): {str(cleanup_error)}", phone)

            # CRITICAL: Always reload from database to get latest session_string
            # This is essential after Railway redeploy to get persisted sessions
            try:
                db_account = self.db.get_account_by_phone(phone)
                if db_account:
                    # Update account with fresh database data
                    account.update(db_account)
                    session_string = db_account.get('session_string')
                    self.log_message(f"📂 Reloaded from DB - session_string: {'✓ Found' if session_string and session_string.strip() else '✗ Not found'} (length: {len(session_string) if session_string else 0})", phone)
                else:
                    session_string = account.get('session_string')
                    self.log_message(f"⚠️ Account not found in DB, using memory", phone)
            except Exception as e:
                self.log_message(f"⚠️ DB reload error: {str(e)}, using memory", phone)
                session_string = account.get('session_string')

            # Validate and use session string
            if session_string and len(session_string.strip()) > 10:  # Valid session strings are longer than 10 chars
                self.log_message(f"✓ Using saved session from database (no code needed)", phone)
                session = StringSession(session_string.strip())
            else:
                if session_string:
                    self.log_message(f"⚠️ Session string too short or invalid, requesting new code", phone)
                else:
                    self.log_message(f"ℹ️ No session found, requesting authentication code", phone)
                # Use empty StringSession for first-time auth
                session = StringSession()

            client = TelegramClient(
                session,
                int(account['api_id']),
                account['api_hash'],
                timeout=20,
                retry_delay=1,
                auto_reconnect=True,
                connection_retries=3
            )

            await asyncio.wait_for(client.connect(), timeout=15.0)

            if not await client.is_user_authorized():
                if session_string:
                    self.log_message(f"⚠️ Saved session expired - requesting new authentication code", phone)
                else:
                    self.log_message(f"New account - sending authentication code", phone)

                account['status'] = 'Waiting for code...'

                await client.send_code_request(phone)

                self.pending_auth[phone] = {
                    'client': client,
                    'account': account,
                    'step': 'code'
                }

                try:
                    socketio.emit('auth_required', {
                        'phone': phone,
                        'step': 'code'
                    })
                    socketio.emit('accounts_updated', self.get_accounts_data())
                except:
                    pass

                return 'auth_required'

            me = await client.get_me()
            self.clients[phone] = client
            account['status'] = 'Connected'

            if session_string:
                self.log_message(f"✓ Connected with saved session: {me.first_name} (no code needed)", phone)
            else:
                self.log_message(f"✓ Connected successfully: {me.first_name}", phone)

            # Save/update session string to database
            try:
                current_session_str = client.session.save()

                if current_session_str and current_session_str != account.get('session_string'):
                    self.db.update_account(phone, {
                        'session_string': current_session_str,
                        'status': 'Connected'
                    })
                    self.log_message(f"✓ Session updated in database for future use", phone)
                    # Reload accounts to get updated data
                    self.accounts = self.load_accounts()
            except Exception as e:
                self.log_message(f"Warning: Could not save session string: {str(e)}", phone)

            try:
                source_entity = await self.get_entity_safe(client, account['source_channel'], phone)
                self.log_message(f"Source channel verified: {source_entity.title if hasattr(source_entity, 'title') else 'Channel'}", phone)
                
                for target_id in account['target_channels']:
                    try:
                        target_entity = await self.get_entity_safe(client, target_id, phone)
                        self.log_message(f"Target channel {target_id} verified: {target_entity.title if hasattr(target_entity, 'title') else 'Channel'}", phone)
                    except Exception as e:
                        self.log_message(f"Warning: Target channel {target_id} not accessible: {str(e)}", phone)
                        
            except Exception as e:
                self.log_message(f"Warning: Could not verify channels: {str(e)}", phone)
            
            return 'success'
            
        except asyncio.TimeoutError:
            self.log_message(f"Connection timeout", phone)
            account['status'] = 'Timeout'
            return 'failed'
            
        except Exception as e:
            error_msg = str(e)
            if "database is locked" in error_msg.lower():
                self.log_message(f"Database locked, retrying...", phone)
                account['status'] = 'Retrying...'
                await asyncio.sleep(3)

                try:
                    # Use saved session string if available
                    retry_session_string = account.get('session_string')
                    if retry_session_string and retry_session_string.strip():
                        retry_session = StringSession(retry_session_string)
                        self.log_message(f"Retry using saved session", phone)
                    else:
                        retry_session = StringSession()

                    retry_client = TelegramClient(
                        retry_session,
                        int(account['api_id']),
                        account['api_hash'],
                        timeout=20,
                        retry_delay=1,
                        auto_reconnect=True,
                        connection_retries=3
                    )

                    await asyncio.wait_for(retry_client.connect(), timeout=15.0)

                    if await retry_client.is_user_authorized():
                        me = await retry_client.get_me()
                        self.clients[phone] = retry_client
                        account['status'] = 'Connected'
                        self.log_message(f"Connected after retry: {me.first_name}", phone)

                        # Save session string
                        try:
                            session_str = retry_client.session.save()
                            if session_str:
                                self.db.update_account(phone, {
                                    'session_string': session_str,
                                    'status': 'Connected'
                                })
                                self.accounts = self.load_accounts()
                        except:
                            pass

                        return 'success'
                    else:
                        account['status'] = 'Auth required after retry'
                        self.log_message(f"Authorization required after retry", phone)
                        await retry_client.send_code_request(phone)
                        
                        self.pending_auth[phone] = {
                            'client': retry_client,
                            'account': account,
                            'step': 'code'
                        }
                        
                        try:
                            socketio.emit('auth_required', {
                                'phone': phone,
                                'step': 'code'
                            })
                        except:
                            pass
                        return 'auth_required'
                        
                except Exception as retry_error:
                    self.log_message(f"Retry failed: {str(retry_error)}", phone)
                    account['status'] = 'Retry failed'
                    return 'failed'
                    
            else:
                self.log_message(f"Connection error: {error_msg}", phone)
                account['status'] = 'Error'
                # Update status in database
                try:
                    self.db.update_account(phone, {'status': 'Error'})
                except:
                    pass
                return 'failed'
    
    def submit_auth_code(self, phone, code):
        if phone not in self.pending_auth:
            return {"success": False, "error": "No pending authentication for this phone"}
        
        auth_data = self.pending_auth[phone]
        
        if self.loop:
            asyncio.run_coroutine_threadsafe(
                self.process_auth_code(auth_data['account'], auth_data['client'], code, phone),
                self.loop
            )
            return {"success": True, "message": "Code submitted"}
        
        return {"success": False, "error": "Async loop not available"}
    
    async def process_auth_code(self, account, client, code, phone):
        try:
            await client.sign_in(phone, code)

            me = await client.get_me()
            self.clients[phone] = client
            account['status'] = 'Connected'
            self.log_message(f"Authentication successful: {me.first_name}", phone)

            # Save session string to database
            try:
                if isinstance(client.session, StringSession):
                    session_str = client.session.save()
                else:
                    session_str = StringSession.save(client.session)

                if session_str:
                    self.db.update_account(phone, {
                        'session_string': session_str,
                        'status': 'Connected'
                    })
                    self.log_message(f"Session saved after authentication", phone)
                    self.accounts = self.load_accounts()
            except Exception as e:
                self.log_message(f"Warning: Could not save session: {str(e)}", phone)

            if phone in self.pending_auth:
                del self.pending_auth[phone]

            try:
                socketio.emit('auth_success', {'phone': phone})
                socketio.emit('accounts_updated', self.get_accounts_data())
            except:
                pass
            
            if self.connection_paused and self.connection_in_progress:
                self.log_message("Resuming connection process...")
                await asyncio.sleep(1)
                await self.resume_connection_after_auth()
            
        except SessionPasswordNeededError:
            self.log_message("2FA password required", phone)
            
            self.pending_auth[phone]['step'] = 'password'
            
            try:
                socketio.emit('auth_required', {
                    'phone': phone,
                    'step': 'password'
                })
            except:
                pass
            
        except Exception as e:
            error_msg = str(e)
            self.log_message(f"Code authentication error: {error_msg}", phone)
            
            if phone in self.pending_auth:
                del self.pending_auth[phone]
            
            account['status'] = 'Auth error'
            
            try:
                socketio.emit('auth_error', {
                    'phone': phone,
                    'error': error_msg
                })
                socketio.emit('accounts_updated', self.get_accounts_data())
            except:
                pass
            
            if self.connection_paused and self.connection_in_progress:
                self.log_message("Resuming connection process after auth error...")
                await asyncio.sleep(1)
                await self.resume_connection_after_auth()
    
    def submit_auth_password(self, phone, password):
        if phone not in self.pending_auth:
            return {"success": False, "error": "No pending authentication for this phone"}
        
        auth_data = self.pending_auth[phone]
        
        if self.loop:
            asyncio.run_coroutine_threadsafe(
                self.process_auth_password(auth_data['account'], auth_data['client'], password, phone),
                self.loop
            )
            return {"success": True, "message": "Password submitted"}
        
        return {"success": False, "error": "Async loop not available"}
    
    async def process_auth_password(self, account, client, password, phone):
        try:
            await client.sign_in(password=password)

            me = await client.get_me()
            self.clients[phone] = client
            account['status'] = 'Connected'
            self.log_message(f"2FA authentication successful: {me.first_name}", phone)

            # Save session string to database
            try:
                if isinstance(client.session, StringSession):
                    session_str = client.session.save()
                else:
                    session_str = StringSession.save(client.session)

                if session_str:
                    self.db.update_account(phone, {
                        'session_string': session_str,
                        'status': 'Connected'
                    })
                    self.log_message(f"Session saved after 2FA", phone)
                    self.accounts = self.load_accounts()
            except Exception as e:
                self.log_message(f"Warning: Could not save session: {str(e)}", phone)

            if phone in self.pending_auth:
                del self.pending_auth[phone]

            try:
                socketio.emit('auth_success', {'phone': phone})
                socketio.emit('accounts_updated', self.get_accounts_data())
            except:
                pass
            
            if self.connection_paused and self.connection_in_progress:
                self.log_message("Resuming connection process...")
                await asyncio.sleep(1)
                await self.resume_connection_after_auth()
            
        except Exception as e:
            self.log_message(f"2FA password error: {str(e)}", phone)
            account['status'] = '2FA error'
            
            if phone in self.pending_auth:
                del self.pending_auth[phone]
            
            try:
                socketio.emit('auth_error', {
                    'phone': phone,
                    'error': str(e)
                })
                socketio.emit('accounts_updated', self.get_accounts_data())
            except:
                pass
            
            if self.connection_paused and self.connection_in_progress:
                self.log_message("Resuming connection process after auth error...")
                await asyncio.sleep(1)
                await self.resume_connection_after_auth()
    
    async def resume_connection_after_auth(self):
        if not self.connection_in_progress or not self.connection_paused:
            return
        
        self.connection_paused = False
        
        current_index = next((i for i, acc in enumerate(self.connection_queue) 
                            if acc['phone'] == self.current_connecting_phone), -1)
        
        if current_index == -1:
            self.finish_connection_process(len(self.clients), 0)
            return
        
        connected_count = len(self.clients)
        failed_count = 0
        
        for index in range(current_index + 1, len(self.connection_queue)):
            if not self.connection_in_progress:
                break
                
            account = self.connection_queue[index]
            phone = account['phone']
            self.current_connecting_phone = phone
            
            self.log_message(f"Continuing with account {index + 1}/{len(self.connection_queue)}: {phone}")
            
            try:
                socketio.emit('connection_progress', {
                    'current': index + 1,
                    'total': len(self.connection_queue),
                    'status': f"Connecting {phone}..."
                })
            except:
                pass
            
            try:
                result = await self.connect_single_account_sequential(account)
                
                if result == 'auth_required':
                    self.log_message(f"Authentication required for {phone}. Process paused again.")
                    self.connection_paused = True
                    
                    try:
                        socketio.emit('connection_progress', {
                            'current': index + 1,
                            'total': len(self.connection_queue),
                            'status': f"Authentication required for {phone}. Process paused.",
                            'paused': True
                        })
                    except:
                        pass
                    return
                    
                elif result == 'success':
                    connected_count += 1
                    self.log_message(f"Successfully connected: {phone}")
                    
                else:
                    failed_count += 1
                    self.log_message(f"Failed to connect: {phone}")
                    
            except Exception as e:
                failed_count += 1
                self.log_message(f"Connection error for {phone}: {str(e)}")
                account['status'] = 'Error'
            
            try:
                socketio.emit('accounts_updated', self.get_accounts_data())
            except:
                pass
            
            if index < len(self.connection_queue) - 1:
                await asyncio.sleep(2)
        
        total_connected = len(self.clients)
        total_failed = len(self.connection_queue) - total_connected
        self.finish_connection_process(total_connected, total_failed)
    
    def scan_all_posts(self):
        if not self.clients:
            return {"success": False, "error": "Connect to accounts first!"}
        
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.perform_scan_all_posts(), self.loop)
        
        self.scan_message("Starting scan of all posts...")
        self.log_message("Starting scan of all posts...")
        
        return {"success": True, "message": "Post scanning started"}
    
    async def perform_scan_all_posts(self):
        self.scanned_ids = {}
        
        for phone, client in self.clients.items():
            try:
                account = next(acc for acc in self.accounts if acc['phone'] == phone)
                await self.scan_channel_posts(account, client)
            except Exception as e:
                self.log_message(f"Scan error: {str(e)}", phone)
        
        self.scan_message(f"Scan completed")
        self.log_message(f"Post scanning completed")
        
        try:
            socketio.emit('scanned_ids_updated', {'ids': self.scanned_ids})
        except:
            pass
    
    async def scan_channel_posts(self, account, client):
        try:
            channel_id = account['source_channel']
            source_entity = await self.get_entity_safe(client, channel_id, account['phone'])
            phone = account['phone']
            
            self.scan_message(f"Scanning channel: {channel_id}", phone)
            
            if phone not in self.scanned_ids:
                self.scanned_ids[phone] = []
            
            message_count = 0
            service_count = 0
            async for message in client.iter_messages(source_entity, limit=None):
                if hasattr(message, '__class__') and 'MessageService' in str(message.__class__):
                    service_count += 1
                    continue
                
                if not message.text and not message.media:
                    service_count += 1
                    continue
                
                message_count += 1
                self.scanned_ids[phone].append(str(message.id))
                self.scan_message(f"Message ID: {message.id}", phone, channel_id)
                
                if message_count % 100 == 0:
                    await asyncio.sleep(0.1)
            
            self.scan_message(f"Scan completed: {message_count} content messages, {service_count} service messages skipped", phone, channel_id)
            self.log_message(f"Channel scan: {message_count} messages ({service_count} skipped)", phone)
            
        except Exception as e:
            error_msg = str(e)
            self.scan_message(f"Channel scan error: {error_msg}", account['phone'])
            self.log_message(f"Channel scan error: {error_msg}", account['phone'])
            
            if "Could not find the input entity" in error_msg:
                channel_id = account['source_channel']
                self.log_message(f"HINT: If channel ID is {channel_id}, try: -100{channel_id}", account['phone'])
                self.scan_message(f"HINT: Try formatting channel ID as: -100{channel_id}", account['phone'])
    
    def add_scheduled_posts(self, post_ids, time_slots, selected_channels):
        if not post_ids:
            return {"success": False, "error": "Enter at least one message ID!"}

        if not time_slots:
            return {"success": False, "error": "Create at least one time slot!"}

        if not selected_channels:
            return {"success": False, "error": "Select at least one channel!"}

        post_ids_list = [id.strip() for id in post_ids.split(',') if id.strip()]
        try:
            post_ids_list = [str(int(pid)) for pid in post_ids_list]
            post_ids_list = list(set(post_ids_list))
        except ValueError:
            return {"success": False, "error": "All message IDs must be valid numbers!"}

        if not post_ids_list:
            return {"success": False, "error": "No valid message IDs provided!"}

        utc_plus_2 = timezone(timedelta(hours=2))
        current_time = datetime.now(utc_plus_2)
        
        created_posts = []
        
        all_channels = []
        for phone, channels in selected_channels.items():
            for channel in channels:
                all_channels.append({'phone': phone, 'channel': channel})
        
        num_channels = len(all_channels)
        
        channel_used_ids = {}
        for ch_info in all_channels:
            channel_key = f"{ch_info['phone']}_{ch_info['channel']}"
            channel_used_ids[channel_key] = set()
        
        for i, time_slot in enumerate(time_slots):
            try:
                # Parse datetime from frontend (user's local time UTC+2)
                slot_datetime = datetime.strptime(time_slot['datetime'], '%Y-%m-%dT%H:%M')
                # User enters time in UTC+2, so we treat it as UTC+2
                slot_datetime = slot_datetime.replace(tzinfo=utc_plus_2)
                # Database will store this as-is (timezone aware)
            except ValueError:
                continue

            time_diff = (slot_datetime - current_time).total_seconds()
            if time_diff < -60:
                continue
            
            channel_posts = {}
            
            for ch_info in all_channels:
                phone = ch_info['phone']
                channel = ch_info['channel']
                channel_key = f"{phone}_{channel}"
                
                available_ids = [pid for pid in post_ids_list if pid not in channel_used_ids[channel_key]]
                
                if not available_ids:
                    channel_used_ids[channel_key].clear()
                    available_ids = post_ids_list.copy()
                
                selected_post_id = random.choice(available_ids)
                
                channel_used_ids[channel_key].add(selected_post_id)
                
                if phone not in channel_posts:
                    channel_posts[phone] = []
                channel_posts[phone].append({'channel': channel, 'post_id': selected_post_id})
            
            scheduled_post_data = {
                "posts": channel_posts,
                "datetime": slot_datetime,
                "status": "Pending"
            }

            # Save to database
            try:
                saved_post = self.db.add_scheduled_post(scheduled_post_data)
                created_posts.append(saved_post)
            except Exception as e:
                self.log_message(f"Failed to save scheduled post: {str(e)}")
        
        if created_posts:
            # Reload scheduled posts from database
            self.scheduled_posts = self.db.get_all_scheduled_posts()

            total_channels = len(all_channels)
            self.log_message(f"Created {len(created_posts)} scheduled posts with {total_channels} channels each")

            if not self.scheduler_running and self.loop and self.clients:
                self.log_message("Auto-starting scheduler for new posts")
                self.scheduler_running = True
                asyncio.run_coroutine_threadsafe(self.run_scheduler(), self.loop)
                try:
                    socketio.emit('scheduler_status', {'running': True})
                except:
                    pass

            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass

            return {"success": True, "message": f"Created {len(created_posts)} scheduled posts!"}
        else:
            return {"success": False, "error": "No valid time slots created!"}    
          
    def get_scheduled_posts_data(self):
        posts_data = []
        utc_plus_2 = timezone(timedelta(hours=2))

        for post in self.scheduled_posts:
            total_channels = sum(len(channels) for channels in post['posts'].values())
            accounts_info = f"{len(post['posts'])} accounts, {total_channels} channels"

            post_ids_display = []
            for phone, channels in post['posts'].items():
                for ch_info in channels:
                    post_ids_display.append(ch_info['post_id'])

            # Convert datetime to UTC+2 for display
            post_datetime = post['datetime']
            if isinstance(post_datetime, datetime):
                # If datetime is naive (no timezone), assume it's UTC and convert to UTC+2
                if post_datetime.tzinfo is None:
                    post_datetime = post_datetime.replace(tzinfo=timezone.utc)
                # Convert to UTC+2
                post_datetime_local = post_datetime.astimezone(utc_plus_2)
            else:
                # If it's a string, parse it
                try:
                    post_datetime = datetime.fromisoformat(str(post_datetime))
                    if post_datetime.tzinfo is None:
                        post_datetime = post_datetime.replace(tzinfo=timezone.utc)
                    post_datetime_local = post_datetime.astimezone(utc_plus_2)
                except:
                    post_datetime_local = post_datetime

            posts_data.append({
                'id': post['id'],
                'time': post_datetime_local.strftime('%d.%m.%Y %H:%M'),
                'post': ', '.join(post_ids_display[:5]) + ('...' if len(post_ids_display) > 5 else ''),
                'accounts': accounts_info,
                'status': post['status']
            })

        return posts_data
    
    def remove_scheduled_post(self, post_id):
        try:
            # Delete from database
            self.db.delete_scheduled_post(post_id)
            # Reload scheduled posts
            self.scheduled_posts = self.db.get_all_scheduled_posts()

            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass

            self.log_message(f"Scheduled post removed: ID {post_id}")
            return {"success": True, "message": "Scheduled post removed"}
        except Exception as e:
            self.logger.error(f"Failed to remove scheduled post: {str(e)}")
            return {"success": False, "error": f"Database error: {str(e)}"}
    
    def start_scheduler(self):
        if not self.scheduled_posts:
            return {"success": False, "error": "No scheduled posts available!"}
        
        if not self.loop:
            return {"success": False, "error": "Async loop not available!"}
        
        if not self.clients:
            return {"success": False, "error": "No accounts connected!"}
        
        self.scheduler_running = True
        
        asyncio.run_coroutine_threadsafe(self.run_scheduler(), self.loop)
        
        pending_posts = [p for p in self.scheduled_posts if p['status'] == 'Pending']
        self.log_message(f"Scheduler started - {len(pending_posts)} pending posts")
        
        try:
            socketio.emit('scheduler_status', {'running': True})
        except:
            pass
        return {"success": True, "message": f"Scheduler started - {len(pending_posts)} pending posts"}
    
    async def run_scheduler(self):
        utc_plus_2 = timezone(timedelta(hours=2))
        self.log_message("Scheduler started - checking every 10 seconds for pending posts")

        while self.scheduler_running:
            try:
                current_time = datetime.now(utc_plus_2)
                self.log_message(f"Scheduler check at: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC+2")

                posts_to_send = []
                for post in self.scheduled_posts:
                    if post['status'] == 'Pending':
                        post_time = post['datetime']

                        # Handle different datetime formats
                        if isinstance(post_time, str):
                            try:
                                post_time = datetime.fromisoformat(post_time)
                            except:
                                try:
                                    post_time = datetime.strptime(post_time, '%Y-%m-%d %H:%M:%S')
                                except:
                                    self.log_message(f"Invalid datetime format for post {post['id']}: {post_time}")
                                    continue

                        # Ensure timezone is set
                        if isinstance(post_time, datetime):
                            if post_time.tzinfo is None:
                                # Assume UTC if no timezone, convert to UTC+2
                                post_time = post_time.replace(tzinfo=timezone.utc).astimezone(utc_plus_2)
                            else:
                                # Convert to UTC+2 for comparison
                                post_time = post_time.astimezone(utc_plus_2)

                        time_diff = (post_time - current_time).total_seconds()
                        self.log_message(f"Post {post['id']}: scheduled for {post_time.strftime('%Y-%m-%d %H:%M:%S')} UTC+2, time diff: {time_diff:.0f} seconds")

                        if time_diff <= 0:
                            posts_to_send.append(post)
                            self.log_message(f"Post {post['id']} ready to send!")
                
                if posts_to_send:
                    self.log_message(f"Found {len(posts_to_send)} posts ready to send")
                    posts_to_send.sort(key=lambda x: x['datetime'] if isinstance(x['datetime'], datetime) else datetime.strptime(x['datetime'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=utc_plus_2))
                    
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
            # First, check if post still exists in database (user might have deleted it)
            try:
                existing_post = self.db.get_scheduled_post_by_id(post['id'])
                if not existing_post:
                    self.log_message(f"⚠️ Post ID {post['id']} was deleted, skipping send")
                    return
                if existing_post.get('status') != 'Pending':
                    self.log_message(f"⚠️ Post ID {post['id']} status is '{existing_post.get('status')}', skipping send")
                    return
            except Exception as e:
                self.log_message(f"⚠️ Could not verify post ID {post['id']}, skipping send: {str(e)}")
                return

            # Update status to Sending
            try:
                self.db.update_scheduled_post(post['id'], {'status': 'Sending'})
                post['status'] = 'Sending'
            except Exception as e:
                self.logger.error(f"Failed to update sending status: {str(e)}")
                return

            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass

            self.log_message(f"Starting to send scheduled post ID {post['id']}")

            success_count = 0
            total_count = 0
            failed_channels = []  # Track failed channels for retry
            error_details = []  # Store detailed error info

            # FIRST ATTEMPT - Send to all channels
            for phone, channels_data in post['posts'].items():
                if phone not in self.clients:
                    self.log_message(f"❌ Account not connected: {phone}")
                    for ch_info in channels_data:
                        total_count += 1
                        error_details.append(f"Account {phone} not connected")
                    continue

                client = self.clients[phone]
                self.log_message(f"📤 Using account {phone} for {len(channels_data)} channels")

                for ch_info in channels_data:
                    channel = ch_info['channel']
                    post_id = ch_info['post_id']
                    total_count += 1

                    try:
                        # Check if client is still connected
                        if not client.is_connected():
                            self.log_message(f"⚠️ Account disconnected, reconnecting...", phone)
                            await client.connect()
                            await asyncio.sleep(2)

                        delay = random.uniform(self.min_delay, self.max_delay)
                        self.log_message(f"⏳ Waiting {delay:.1f}s before sending to channel {channel}", phone)
                        await asyncio.sleep(delay)

                        await self.send_single_scheduled_post(client, post_id, channel, phone)
                        success_count += 1
                        self.log_message(f"✅ Successfully sent post {post_id} to channel {channel}", phone)

                    except FloodWaitError as e:
                        error_msg = f"FloodWait {e.seconds}s - Telegram rate limit"
                        self.log_message(f"⏸️ {error_msg} for channel {channel}", phone)
                        failed_channels.append({'phone': phone, 'channel': channel, 'post_id': post_id, 'error': error_msg, 'wait_time': e.seconds})
                        error_details.append(f"Channel {channel}: {error_msg}")
                    except ChannelPrivateError:
                        error_msg = "Channel is private or account not member"
                        self.log_message(f"🔒 {error_msg}: {channel}", phone)
                        error_details.append(f"Channel {channel}: {error_msg}")
                    except UserBannedInChannelError:
                        error_msg = "User is banned in this channel"
                        self.log_message(f"🚫 {error_msg}: {channel}", phone)
                        error_details.append(f"Channel {channel}: {error_msg}")
                    except ValueError as e:
                        error_msg = str(e)
                        self.log_message(f"⚠️ Validation error for channel {channel}: {error_msg}", phone)
                        if "not accessible" not in error_msg.lower():
                            failed_channels.append({'phone': phone, 'channel': channel, 'post_id': post_id, 'error': error_msg, 'wait_time': 0})
                        error_details.append(f"Channel {channel}: {error_msg}")
                    except TimeoutError as e:
                        error_msg = f"Timeout error: {str(e)}"
                        self.log_message(f"⏱️ {error_msg} for channel {channel}", phone)
                        failed_channels.append({'phone': phone, 'channel': channel, 'post_id': post_id, 'error': error_msg, 'wait_time': 0})
                        error_details.append(f"Channel {channel}: {error_msg}")
                    except Exception as e:
                        error_type = type(e).__name__
                        error_msg = f"{error_type}: {str(e)}"
                        self.log_message(f"❌ Failed channel {channel}: {error_msg}", phone)
                        failed_channels.append({'phone': phone, 'channel': channel, 'post_id': post_id, 'error': error_msg, 'wait_time': 0})
                        error_details.append(f"Channel {channel}: {error_msg}")

            # RETRY MECHANISM - Retry failed channels with exponential backoff
            if failed_channels:
                self.log_message(f"🔄 RETRY: {len(failed_channels)} channels failed, attempting retry...")
                retry_delay = 30  # Start with 30 seconds
                max_retries = 2

                for retry_attempt in range(max_retries):
                    if not failed_channels:
                        break

                    self.log_message(f"🔄 Retry attempt {retry_attempt + 1}/{max_retries} for {len(failed_channels)} channels")
                    await asyncio.sleep(retry_delay)

                    still_failed = []
                    for fail_info in failed_channels:
                        phone = fail_info['phone']
                        channel = fail_info['channel']
                        post_id = fail_info['post_id']

                        # Skip if account not connected
                        if phone not in self.clients:
                            still_failed.append(fail_info)
                            continue

                        client = self.clients[phone]

                        try:
                            # Check and reconnect if needed
                            if not client.is_connected():
                                self.log_message(f"🔌 Reconnecting account for retry...", phone)
                                await client.connect()
                                await asyncio.sleep(2)

                            self.log_message(f"🔄 Retrying channel {channel}", phone)
                            await self.send_single_scheduled_post(client, post_id, channel, phone)
                            success_count += 1
                            self.log_message(f"✅ RETRY SUCCESS: Post {post_id} sent to channel {channel}", phone)
                            # Remove from error_details if retry successful
                            error_details = [e for e in error_details if f"Channel {channel}" not in e]

                        except Exception as e:
                            error_type = type(e).__name__
                            self.log_message(f"❌ RETRY FAILED: Channel {channel} - {error_type}: {str(e)}", phone)
                            still_failed.append(fail_info)

                    failed_channels = still_failed
                    retry_delay *= 2  # Exponential backoff

                if failed_channels:
                    self.log_message(f"⚠️ {len(failed_channels)} channels still failed after {max_retries} retries")
            
            # Update status in database with detailed error info
            if success_count == total_count and total_count > 0:
                new_status = 'Sent'
                self.log_message(f"✅ Post {post['id']} FULLY SENT: {success_count}/{total_count} channels successful")
                error_message = None
            elif success_count > 0:
                new_status = f'Partial ({success_count}/{total_count})'
                self.log_message(f"⚠️ Post {post['id']} PARTIALLY SENT: {success_count}/{total_count} channels successful")
                error_message = "; ".join(error_details[:10])  # Store up to 10 error details
                if len(error_details) > 10:
                    error_message += f"... and {len(error_details) - 10} more errors"
                self.log_message(f"📋 Failed channels details: {error_message}")
            else:
                new_status = 'Error'
                self.log_message(f"❌ Post {post['id']} FAILED: 0/{total_count} channels successful")
                error_message = "; ".join(error_details[:10])
                if len(error_details) > 10:
                    error_message += f"... and {len(error_details) - 10} more errors"
                self.log_message(f"📋 All errors: {error_message}")

            # Update in database with error details
            try:
                update_data = {
                    'status': new_status,
                    'sent_at': datetime.now(timezone(timedelta(hours=2)))
                }
                if error_message:
                    update_data['error_message'] = error_message

                self.db.update_scheduled_post(post['id'], update_data)
                post['status'] = new_status
                # Reload scheduled posts
                self.scheduled_posts = self.db.get_all_scheduled_posts()
            except Exception as e:
                self.logger.error(f"Failed to update post status: {str(e)}")

            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass
            
        except Exception as e:
            self.log_message(f"Scheduled post general error: {str(e)}")
            # Update error status in database
            try:
                self.db.update_scheduled_post(post['id'], {
                    'status': 'Error',
                    'error_message': str(e)
                })
                post['status'] = 'Error'
                self.scheduled_posts = self.db.get_all_scheduled_posts()
            except Exception as db_error:
                self.logger.error(f"Failed to update error status: {str(db_error)}")

            try:
                socketio.emit('scheduled_posts_updated', self.get_scheduled_posts_data())
            except:
                pass
    
    async def send_single_scheduled_post(self, client, post_input, target_channel, phone):
        try:
            # Validate account exists
            account = next((acc for acc in self.accounts if acc['phone'] == phone), None)
            if not account:
                raise ValueError(f"Account {phone} not found in accounts list")

            source_channel_id = int(account['source_channel'])
            message_id = int(post_input)
            target_channel_id = int(target_channel)

            # Get source channel entity with detailed error
            try:
                source_entity = await asyncio.wait_for(
                    self.get_entity_safe(client, source_channel_id, phone),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                raise ValueError(f"Timeout getting source channel {source_channel_id}")
            except Exception as e:
                error_type = type(e).__name__
                raise ValueError(f"Source channel {source_channel_id} not accessible ({error_type}: {str(e)})")

            # Get target channel entity with detailed error
            try:
                target_entity = await asyncio.wait_for(
                    self.get_entity_safe(client, target_channel_id, phone),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                raise ValueError(f"Timeout getting target channel {target_channel_id}")
            except Exception as e:
                error_type = type(e).__name__
                raise ValueError(f"Target channel {target_channel_id} not accessible ({error_type}: {str(e)})")

            # Get message from source channel with validation
            try:
                message = await asyncio.wait_for(
                    client.get_messages(source_entity, ids=message_id),
                    timeout=15.0
                )
                if not message:
                    raise ValueError(f"Message {message_id} not found in source channel {source_channel_id}")

                if hasattr(message, '__class__') and 'MessageService' in str(message.__class__):
                    raise ValueError(f"Message {message_id} is a service message (cannot be forwarded)")

                if not message.text and not message.media:
                    raise ValueError(f"Message {message_id} is empty (no text or media)")
            except asyncio.TimeoutError:
                raise ValueError(f"Timeout getting message {message_id} from source channel")
            except Exception as e:
                if "ValueError" not in str(type(e)):
                    error_type = type(e).__name__
                    raise ValueError(f"Cannot get message {message_id} ({error_type}: {str(e)})")
                raise

            # Forward message with timeout and detailed error handling
            try:
                await asyncio.wait_for(
                    client.forward_messages(
                        target_entity,
                        message,
                        from_peer=source_entity,
                        drop_author=True,
                        silent=True
                    ),
                    timeout=20.0
                )
            except asyncio.TimeoutError:
                raise ValueError(f"Timeout forwarding message {message_id} to channel {target_channel_id}")
            except FloodWaitError as e:
                # Re-raise FloodWaitError as is (will be caught in parent)
                raise
            except ChannelPrivateError:
                # Re-raise ChannelPrivateError as is
                raise
            except UserBannedInChannelError:
                # Re-raise UserBannedInChannelError as is
                raise
            except Exception as e:
                error_type = type(e).__name__
                raise ValueError(f"Forward failed for message {message_id} to channel {target_channel_id} ({error_type}: {str(e)})")

        except FloodWaitError:
            raise
        except ChannelPrivateError:
            raise
        except UserBannedInChannelError:
            raise
        except ValueError:
            raise
        except Exception as e:
            error_type = type(e).__name__
            raise ValueError(f"Unexpected error in send_single_scheduled_post ({error_type}: {str(e)})")
    
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
    utc_plus_2 = timezone(timedelta(hours=2))
    current_time = datetime.now(utc_plus_2)
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
