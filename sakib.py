#!/usr/bin/env python3
"""
Professional Telegram Video Download Bot
Features:
- Video downloads from multiple platforms (YouTube, TikTok, Instagram, etc.)
- User data tracking and statistics
- Admin panel with 'I AM BOSS' command
- Professional interface with command menus
"""

import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, Message, InlineKeyboardMarkup, InlineKeyboardButton
import yt_dlp
import os
import json
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import threading
import tempfile
import logging
import sys

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Bot Configuration - Fixed bot token handling
BOT_TOKEN = os.getenv("BOT_TOKEN", "8009141492:AAExxn6q_56liioGo--NLu1P0k4ra7YlyM8")
ADMIN_PIN = os.getenv("ADMIN_PIN", "872398")  # Admin PIN - use env var or fallback

DATA_FILE = "user_data.json"
DOWNLOADS_DIR = "downloads"
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB limit for Telegram

# Validate bot token
if not BOT_TOKEN:
    logger.error("BOT_TOKEN environment variable is required but not set")
    sys.exit(1)

# Initialize bot
bot = telebot.TeleBot(BOT_TOKEN)

class UserDataManager:
    """Manages user data storage and statistics"""
    
    def __init__(self):
        self.data_file = DATA_FILE
        self.user_data = self.load_data()
        self.lock = threading.Lock()  # Thread safety
    
    def load_data(self) -> Dict[str, Any]:
        """Load user data from JSON file"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading data: {e}")
        return {
            "users": {},
            "bot_stats": {
                "total_downloads": 0,
                "total_users": 0,
                "start_date": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat()
            },
            "admin_settings": {
                "message_deletion_time": 300,  # Default 5 minutes
                "auto_delete_enabled": True,
                "banned_words": [],  # Admin configurable banned words
                "banned_words_enabled": True,
                "channel_join_required": True,
                "channel_join_after_links": 5,  # Require join after N links
                "promotion_channel": "https://t.me/follwnowo",  # Default promotion channel
                "help_channel": "https://t.me/+enYm2HitF0BkNTZl",  # Default help channel
                "max_file_size_mb": 200,  # Configurable file size limit
                "auto_removal_enabled": True,  # NEW: Enable auto-removal of files
                "base_removal_time_minutes": 30,  # NEW: Base time for 200MB files (30 minutes)
            }
        }
    
    def save_data(self):
        """Save user data to JSON file with thread safety and atomic writes"""
        with self.lock:
            try:
                self.user_data["bot_stats"]["last_updated"] = datetime.now().isoformat()
                
                # Atomic write using temporary file
                temp_file = f"{self.data_file}.tmp"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.user_data, f, indent=2, ensure_ascii=False)
                
                # Atomic rename
                os.rename(temp_file, self.data_file)
            except Exception as e:
                logger.error(f"Error saving data: {e}")
                # Clean up temp file if it exists
                if os.path.exists(f"{self.data_file}.tmp"):
                    try:
                        os.remove(f"{self.data_file}.tmp")
                    except:
                        pass
    
    def add_user(self, user_id: int, username: Optional[str] = None, first_name: Optional[str] = None) -> None:
        """Add or update user information"""
        user_id_str = str(user_id)
        current_time = datetime.now().isoformat()
        
        if user_id_str not in self.user_data["users"]:
            self.user_data["users"][user_id_str] = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "join_date": current_time,
                "last_activity": current_time,
                "downloads": [],
                "total_downloads": 0,
                "commands_used": {},
                "status": "active",
                "link_count": 0,  # Track total links sent by user
                "all_links": [],  # Store all links with timestamps
                "channel_joined": False,  # Track if user joined required channel
                "last_channel_check": None  # Last time we checked channel membership
            }
            self.user_data["bot_stats"]["total_users"] += 1
        else:
            # Update existing user info
            self.user_data["users"][user_id_str]["username"] = username
            self.user_data["users"][user_id_str]["first_name"] = first_name
            self.user_data["users"][user_id_str]["last_activity"] = current_time
        
        self.save_data()
    
    def log_download(self, user_id: int, url: str, title: str, platform: str, file_size_mb: float = 0.0):
        """Log a download for a user with file size tracking"""
        user_id_str = str(user_id)
        download_info = {
            "url": url,
            "title": title,
            "platform": platform,
            "file_size_mb": file_size_mb,
            "timestamp": datetime.now().isoformat()
        }
        
        if user_id_str in self.user_data["users"]:
            self.user_data["users"][user_id_str]["downloads"].append(download_info)
            self.user_data["users"][user_id_str]["total_downloads"] += 1
            
            # Track total MB downloaded
            if "total_mb_downloaded" not in self.user_data["users"][user_id_str]:
                self.user_data["users"][user_id_str]["total_mb_downloaded"] = 0.0
            self.user_data["users"][user_id_str]["total_mb_downloaded"] += file_size_mb
        
        self.user_data["bot_stats"]["total_downloads"] += 1
        
        # Track total MB in bot stats
        if "total_mb_downloaded" not in self.user_data["bot_stats"]:
            self.user_data["bot_stats"]["total_mb_downloaded"] = 0.0
        self.user_data["bot_stats"]["total_mb_downloaded"] += file_size_mb
        
        self.save_data()
    
    def log_link(self, user_id: int, url: str) -> Dict[str, Any]:
        """Log a link sent by user and return link tracking info"""
        user_id_str = str(user_id)
        current_time = datetime.now().isoformat()
        
        link_info = {
            "url": url,
            "timestamp": current_time,
            "type": self._detect_platform(url)
        }
        
        if user_id_str in self.user_data["users"]:
            # Ensure all new fields exist for backward compatibility
            user_data = self.user_data["users"][user_id_str]
            if "link_count" not in user_data:
                user_data["link_count"] = 0
            if "all_links" not in user_data:
                user_data["all_links"] = []
            if "channel_joined" not in user_data:
                user_data["channel_joined"] = False
            if "last_channel_check" not in user_data:
                user_data["last_channel_check"] = None
                
            # Log the link
            user_data["all_links"].append(link_info)
            user_data["link_count"] += 1
            user_data["last_activity"] = current_time
            
            self.save_data()
            
            # Return tracking info for channel join check
            admin_settings = self.user_data.get("admin_settings", {})
            link_threshold = admin_settings.get("channel_join_after_links", 5)
            channel_required = admin_settings.get("channel_join_required", True)
            
            return {
                "link_count": user_data["link_count"],
                "needs_channel_join": (
                    channel_required and 
                    user_data["link_count"] >= link_threshold and 
                    not user_data["channel_joined"]
                ),
                "threshold": link_threshold
            }
        
        return {"link_count": 0, "needs_channel_join": False, "threshold": 5}
    
    def _detect_platform(self, url: str) -> str:
        """Detect platform from URL"""
        url_lower = url.lower()
        if 'youtube.com' in url_lower or 'youtu.be' in url_lower:
            return 'YouTube'
        elif 'tiktok.com' in url_lower:
            return 'TikTok'
        elif 'instagram.com' in url_lower:
            return 'Instagram'
        elif 'twitter.com' in url_lower or 'x.com' in url_lower:
            return 'Twitter/X'
        elif 'facebook.com' in url_lower:
            return 'Facebook'
        else:
            return 'Other'
    
    def update_channel_join_status(self, user_id: int, joined: bool) -> None:
        """Update user's channel join status"""
        user_id_str = str(user_id)
        if user_id_str in self.user_data["users"]:
            self.user_data["users"][user_id_str]["channel_joined"] = joined
            self.user_data["users"][user_id_str]["last_channel_check"] = datetime.now().isoformat()
            self.save_data()
    
    def add_banned_word(self, word: str) -> bool:
        """Add a word to banned words list"""
        if "admin_settings" not in self.user_data:
            self.user_data["admin_settings"] = {}
        if "banned_words" not in self.user_data["admin_settings"]:
            self.user_data["admin_settings"]["banned_words"] = []
            
        word = word.lower().strip()
        if word and word not in self.user_data["admin_settings"]["banned_words"]:
            self.user_data["admin_settings"]["banned_words"].append(word)
            self.save_data()
            return True
        return False
    
    def remove_banned_word(self, word: str) -> bool:
        """Remove a word from banned words list"""
        if "admin_settings" not in self.user_data:
            return False
        if "banned_words" not in self.user_data["admin_settings"]:
            return False
            
        word = word.lower().strip()
        if word in self.user_data["admin_settings"]["banned_words"]:
            self.user_data["admin_settings"]["banned_words"].remove(word)
            self.save_data()
            return True
        return False
    
    def get_banned_words(self) -> List[str]:
        """Get list of banned words"""
        admin_settings = self.user_data.get("admin_settings", {})
        return admin_settings.get("banned_words", [])
    
    def check_banned_words(self, text: str) -> List[str]:
        """Check if text contains banned words"""
        if not text:
            return []
            
        admin_settings = self.user_data.get("admin_settings", {})
        banned_words = admin_settings.get("banned_words", [])
        banned_words_enabled = admin_settings.get("banned_words_enabled", True)
        
        if not banned_words_enabled:
            return []
            
        text_lower = text.lower()
        found_words = []
        
        for word in banned_words:
            if word.lower() in text_lower:
                found_words.append(word)
                
        return found_words
    
    def log_command(self, user_id: int, command: str):
        """Log command usage"""
        user_id_str = str(user_id)
        if user_id_str in self.user_data["users"]:
            commands = self.user_data["users"][user_id_str]["commands_used"]
            commands[command] = commands.get(command, 0) + 1
            self.save_data()
    
    def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Get statistics for a specific user"""
        user_id_str = str(user_id)
        return self.user_data["users"].get(user_id_str, {})
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Get comprehensive bot statistics"""
        return self.user_data
    
    def update_admin_setting(self, setting_key: str, setting_value: Any) -> None:
        """Update admin panel settings"""
        if "admin_settings" not in self.user_data:
            self.user_data["admin_settings"] = {
                "message_deletion_time": 300,
                "auto_delete_enabled": True
            }
        
        self.user_data["admin_settings"][setting_key] = setting_value
        self.save_data()
        logger.info(f"Updated admin setting {setting_key} to {setting_value}")
    
    def get_admin_setting(self, setting_key: str, default_value: Any = None) -> Any:
        """Get admin panel setting"""
        admin_settings = self.user_data.get("admin_settings", {})
        return admin_settings.get(setting_key, default_value)

    # NEW: User account management functions
    def remove_user_account(self, user_id: int) -> bool:
        """Remove a user account completely"""
        user_id_str = str(user_id)
        if user_id_str in self.user_data["users"]:
            # Log the removal
            removed_user = self.user_data["users"][user_id_str]
            logger.info(f"Removing user account: {removed_user.get('first_name', 'Unknown')} (ID: {user_id})")
            
            # Update stats
            if self.user_data["bot_stats"]["total_users"] > 0:
                self.user_data["bot_stats"]["total_users"] -= 1
            
            # Remove the user
            del self.user_data["users"][user_id_str]
            self.save_data()
            return True
        return False
    
    def ban_user_account(self, user_id: int) -> bool:
        """Ban a user account (set status to banned)"""
        user_id_str = str(user_id)
        if user_id_str in self.user_data["users"]:
            self.user_data["users"][user_id_str]["status"] = "banned"
            self.user_data["users"][user_id_str]["ban_date"] = datetime.now().isoformat()
            self.save_data()
            logger.info(f"Banned user account: {user_id}")
            return True
        return False
    
    def unban_user_account(self, user_id: int) -> bool:
        """Unban a user account (set status to active)"""
        user_id_str = str(user_id)
        if user_id_str in self.user_data["users"]:
            self.user_data["users"][user_id_str]["status"] = "active"
            if "ban_date" in self.user_data["users"][user_id_str]:
                del self.user_data["users"][user_id_str]["ban_date"]
            self.save_data()
            logger.info(f"Unbanned user account: {user_id}")
            return True
        return False

# Initialize data manager
data_manager = UserDataManager()

# NEW: File Auto-Removal System
class FileAutoRemovalManager:
    """Manages automatic file removal based on size and time"""
    
    def __init__(self):
        self.scheduled_removals = {}
        self.lock = threading.Lock()
    
    def calculate_removal_time(self, file_size_bytes: int, base_time_minutes: int = 30) -> int:
        """
        Calculate removal time based on file size ranges
        100-200 MB files: 30 minutes
        0-100 MB files: 15-20 minutes maximum (randomly distributed)
        """
        if file_size_bytes <= 0:
            return 15  # Fallback for invalid sizes
        
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        if file_size_mb >= 100:
            # Files 100-200 MB get exactly 30 minutes
            removal_time_minutes = 30
        else:
            # Files 0-100 MB get 15-20 minutes (randomly distributed)
            import random
            removal_time_minutes = random.randint(15, 20)
        
        logger.info(f"File size: {file_size_mb:.1f}MB -> Removal time: {removal_time_minutes} minutes")
        return removal_time_minutes
    
    def schedule_file_removal(self, file_path: str, file_size_bytes: int) -> dict:
        """Schedule a file for automatic removal"""
        if not data_manager.get_admin_setting("auto_removal_enabled", True):
            logger.info(f"Auto-removal disabled, skipping: {file_path}")
            return {"scheduled": False, "reason": "Auto-removal disabled"}
        
        base_time = data_manager.get_admin_setting("base_removal_time_minutes", 30)
        removal_time_minutes = self.calculate_removal_time(file_size_bytes, base_time)
        
        # Calculate actual removal timestamp
        removal_timestamp = datetime.now() + timedelta(minutes=removal_time_minutes)
        
        with self.lock:
            self.scheduled_removals[file_path] = {
                "file_path": file_path,
                "file_size": file_size_bytes,
                "scheduled_time": removal_timestamp.isoformat(),
                "removal_minutes": removal_time_minutes,
                "timer": None
            }
        
        def remove_file():
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Auto-removed file: {file_path} (Size: {file_size_bytes/(1024*1024):.1f}MB)")
                
                with self.lock:
                    if file_path in self.scheduled_removals:
                        del self.scheduled_removals[file_path]
            except Exception as e:
                logger.error(f"Error auto-removing file {file_path}: {e}")
        
        # Schedule the removal
        timer = threading.Timer(removal_time_minutes * 60, remove_file)
        timer.start()
        
        with self.lock:
            self.scheduled_removals[file_path]["timer"] = timer
        
        logger.info(f"Scheduled file removal: {file_path} in {removal_time_minutes} minutes")
        
        return {
            "scheduled": True,
            "file_path": file_path,
            "removal_minutes": removal_time_minutes,
            "removal_timestamp": removal_timestamp.isoformat()
        }
    
    def cancel_file_removal(self, file_path: str) -> bool:
        """Cancel a scheduled file removal"""
        with self.lock:
            if file_path in self.scheduled_removals:
                timer = self.scheduled_removals[file_path].get("timer")
                if timer:
                    timer.cancel()
                del self.scheduled_removals[file_path]
                logger.info(f"Cancelled file removal: {file_path}")
                return True
        return False
    
    def get_scheduled_removals(self) -> dict:
        """Get all scheduled removals"""
        with self.lock:
            return dict(self.scheduled_removals)

# Initialize file auto-removal manager
file_removal_manager = FileAutoRemovalManager()

# Channel membership checking functionality
def check_channel_membership(user_id: int, channel_username: str) -> bool:
    """Check if user is a member of the specified channel with graceful fallback"""
    try:
        # Remove @ and https://t.me/ prefixes if present
        if channel_username.startswith('https://t.me/'):
            channel_username = channel_username.replace('https://t.me/', '')
        if channel_username.startswith('@'):
            channel_username = channel_username[1:]
            
        # FIXED: Handle different URL formats properly
        if channel_username.startswith('+'):
            # Private channel with invite link
            # For private channels, we can't directly check membership
            # We'll return False to prompt manual verification
            logger.info(f"Private channel detected: {channel_username} - requiring manual verification")
            return False
            
        # Try to get chat member status
        member = bot.get_chat_member(f"@{channel_username}", user_id)
        # User is considered a member if they have any status except 'left' or 'kicked'
        return member.status in ['member', 'administrator', 'creator']
        
    except Exception as e:
        error_msg = str(e).lower()
        
        # Handle specific API errors gracefully
        if "member list is inaccessible" in error_msg:
            logger.warning(f"Channel @{channel_username} has restricted member list - using fallback verification for user {user_id}")
            # For private channels, we'll rely on user confirmation
            # This is a limitation of Telegram's API for private channels
            return False  # Will prompt user to verify manually
            
        elif "chat not found" in error_msg:
            logger.error(f"Channel @{channel_username} not found - please check channel URL")
            return False
            
        elif "user not found" in error_msg:
            logger.warning(f"User {user_id} not found when checking membership")
            return False
            
        else:
            logger.error(f"Unexpected error checking channel membership for user {user_id} in @{channel_username}: {e}")
            return False

def create_channel_join_keyboard(promotion_channel: str, help_channel: str) -> InlineKeyboardMarkup:
    """Create keyboard for channel join requirement"""
    keyboard = InlineKeyboardMarkup()
    
    # Add channel join buttons
    if promotion_channel:
        keyboard.add(
            InlineKeyboardButton(
                "🔔 Join Promotion Channel", 
                url=promotion_channel
            )
        )
    
    if help_channel:
        keyboard.add(
            InlineKeyboardButton(
                "💬 Join Help Channel", 
                url=help_channel
            )
        )
    
    # Add verification button
    keyboard.add(
        InlineKeyboardButton(
            "✅ I Joined - Continue Download", 
            callback_data="verify_channel_join"
        )
    )
    
    return keyboard

def handle_banned_words_detection(message: Message) -> bool:
    """Check for banned words and auto-delete if found"""
    if not message.text:
        return False
        
    found_words = data_manager.check_banned_words(message.text)
    
    if found_words:
        try:
            # Delete the message
            bot.delete_message(message.chat.id, message.message_id)
            
            # Send warning message
            warning_msg = bot.send_message(
                message.chat.id,
                f"⚠️ **Message Deleted - Banned Words Detected**\n\n"
                f"Your message contained prohibited words: {', '.join(found_words)}\n\n"
                f"Please follow the community guidelines.",
                parse_mode='Markdown'
            )
            
            # Schedule deletion of warning message
            schedule_message_deletion(message.chat.id, warning_msg.message_id, 30)
            
            user_id = message.from_user.id if message.from_user else "unknown"
            logger.info(f"Deleted message from user {user_id} for banned words: {found_words}")
            return True
            
        except Exception as e:
            logger.error(f"Error handling banned words: {e}")
    
    return False

class VideoDownloader:
    """Handles video downloading with yt-dlp"""
    
    def __init__(self):
        self.downloads_dir = DOWNLOADS_DIR
        os.makedirs(self.downloads_dir, exist_ok=True)
    
    def get_video_info(self, url: str) -> Optional[Dict[str, Any]]:
        """Get video information without downloading"""
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if info:
                    return {
                        'title': info.get('title', 'Unknown'),
                        'uploader': info.get('uploader', 'Unknown'),
                        'duration': info.get('duration', 0),
                        'platform': info.get('extractor', 'Unknown'),
                        'thumbnail': info.get('thumbnail', ''),
                        'formats_available': len(info.get('formats', []))
                    }
        except Exception as e:
            logger.error(f"Error getting video info: {e}")
        return None
    
    def download_video(self, url: str, user_id: int) -> Dict[str, Any]:
        """Download video and return file information"""
        timestamp = int(time.time())
        # Use a safe, short filename template to avoid filesystem limits
        output_template = f"{self.downloads_dir}/video_{user_id}_{timestamp}.%(ext)s"
        
        # Get admin file size limit
        admin_max_size_mb = data_manager.get_admin_setting("max_file_size_mb", 200)
        max_size_bytes = min(admin_max_size_mb * 1024 * 1024, MAX_FILE_SIZE)
        
        ydl_opts = {
            'outtmpl': output_template,
            'format': f'best[filesize<{max_size_bytes}]/worst',  # Use admin limit
            'quiet': True,
            'no_warnings': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                
                # Find the downloaded file
                downloaded_file = None
                for file in os.listdir(self.downloads_dir):
                    if f"video_{user_id}_{timestamp}" in file:
                        downloaded_file = os.path.join(self.downloads_dir, file)
                        break
                
                if downloaded_file and os.path.exists(downloaded_file):
                    file_size = os.path.getsize(downloaded_file)
                    
                    # NEW: Schedule auto-removal for the downloaded file
                    removal_info = file_removal_manager.schedule_file_removal(downloaded_file, file_size)
                    
                    return {
                        'success': True,
                        'file_path': downloaded_file,
                        'title': info.get('title', 'Unknown') if info else 'Unknown',
                        'platform': info.get('extractor', 'Unknown') if info else 'Unknown',
                        'file_size': file_size,
                        'duration': info.get('duration', 0) if info else 0,
                        'removal_info': removal_info  # NEW: Include removal information
                    }
                else:
                    return {'success': False, 'error': 'File not found after download'}
                    
        except Exception as e:
            logger.error(f"Download error: {e}")
            return {'success': False, 'error': str(e)}
    
    def cleanup_file(self, file_path: str) -> None:
        """Clean up downloaded file and cancel its auto-removal"""
        try:
            # Cancel scheduled auto-removal first
            file_removal_manager.cancel_file_removal(file_path)
            
            # Remove file immediately
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Manually cleaned up file: {file_path}")
        except Exception as e:
            logger.error(f"Error cleaning up file: {e}")

# Initialize downloader
downloader = VideoDownloader()

def create_main_menu() -> ReplyKeyboardMarkup:
    """Create main menu keyboard"""
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        KeyboardButton("📥 Download Video"),
        KeyboardButton("📊 My Stats")
    )
    markup.add(
        KeyboardButton("ℹ️ Help"),
        KeyboardButton("🔗 Supported Sites")
    )
    markup.add(
        KeyboardButton("🧹 Clear Chat")
    )
    return markup

def format_duration(seconds):
    """Format duration in human readable format"""
    if not seconds:
        return "Unknown"
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_file_size(bytes_size):
    """Format file size in human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"

def escape_markdown(text):
    """Escape special markdown characters to prevent parsing errors"""
    if not text or not isinstance(text, str):
        return str(text) if text is not None else "Unknown"
    
    # Only escape the most critical markdown characters that cause issues
    escaped_text = str(text)
    escaped_text = escaped_text.replace('*', '\\*')
    escaped_text = escaped_text.replace('_', '\\_')
    escaped_text = escaped_text.replace('[', '\\[')
    escaped_text = escaped_text.replace(']', '\\]')
    escaped_text = escaped_text.replace('`', '\\`')
    
    return escaped_text

# Thread-safe message deletion management
message_deletion_lock = threading.Lock()
pending_message_deletions = {}  # Track pending deletions

def schedule_message_deletion(chat_id: int, message_id: int, delay_seconds: Optional[int] = None):
    """Schedule a message for deletion after specified delay"""
    # Check if auto-delete is enabled first
    all_stats = data_manager.get_all_stats()
    admin_settings = all_stats.get("admin_settings", {})
    auto_delete_enabled = admin_settings.get("auto_delete_enabled", True)
    
    if not auto_delete_enabled:
        logger.info(f"Auto-delete disabled, skipping deletion of message {message_id}")
        return
    
    if delay_seconds is None:
        delay_seconds = int(admin_settings.get("message_deletion_time", 300))  # Default 5 minutes
    
    def delete_message():
        try:
            with message_deletion_lock:
                # Remove from pending deletions
                key = f"{chat_id}_{message_id}"
                if key in pending_message_deletions:
                    del pending_message_deletions[key]
            
            bot.delete_message(chat_id, message_id)
            logger.info(f"Auto-deleted message {message_id} in chat {chat_id}")
        except Exception as e:
            logger.warning(f"Could not delete message {message_id}: {e}")
    
    # Track the deletion
    key = f"{chat_id}_{message_id}"
    with message_deletion_lock:
        pending_message_deletions[key] = time.time() + delay_seconds
    
    # Schedule deletion
    timer = threading.Timer(delay_seconds, delete_message)
    timer.start()

# Global state tracking
waiting_for_admin_pin = {}
waiting_for_banned_word = {}
waiting_for_promotion_channel = {}
waiting_for_help_channel = {}
waiting_for_new_pin = {}
waiting_for_admin_message = {}
user_pending_downloads = {}

def create_admin_settings_keyboard():
    """Create admin settings keyboard"""
    keyboard = InlineKeyboardMarkup()
    
    # Main admin functions
    keyboard.add(
        InlineKeyboardButton("🔄 Refresh Dashboard", callback_data="admin_refresh"),
        InlineKeyboardButton("👥 User Management", callback_data="admin_user_list")
    )
    
    keyboard.add(
        InlineKeyboardButton("⏰ Message Settings", callback_data="admin_msg_settings"),
        InlineKeyboardButton("🚫 Banned Words", callback_data="admin_banned_words")
    )
    
    keyboard.add(
        InlineKeyboardButton("📺 Channel Settings", callback_data="admin_channel_settings"),
        InlineKeyboardButton("📊 Link Analytics", callback_data="admin_link_analytics")
    )
    
    # NEW: Additional admin features  
    keyboard.add(
        InlineKeyboardButton("🔐 Change PIN", callback_data="admin_change_pin"),
        InlineKeyboardButton("🗂️ File Management", callback_data="admin_file_management")
    )
    
    keyboard.add(
        InlineKeyboardButton("🗑️ Remove All Downloads", callback_data="admin_remove_all_downloads")
    )
    
    keyboard.add(
        InlineKeyboardButton("💬 Chat with Users", callback_data="admin_chat_user")
    )
    
    return keyboard

def create_message_deletion_settings_keyboard():
    """Create message deletion settings keyboard"""
    keyboard = InlineKeyboardMarkup()
    
    # Time options in seconds
    times = [
        (60, "1 minute"),
        (300, "5 minutes"),
        (600, "10 minutes"), 
        (900, "15 minutes"),
        (1800, "30 minutes"),
        (3600, "1 hour"),
        (0, "Disable auto-delete")
    ]
    
    for seconds, label in times:
        keyboard.add(
            InlineKeyboardButton(
                f"⏰ {label}",
                callback_data=f"set_deletion_time_{seconds}"
            )
        )
    
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main"))
    return keyboard

def create_user_list_keyboard(users_data: Dict[str, Any], page: int = 0, page_size: int = 10):
    """Create user list keyboard with pagination"""
    keyboard = InlineKeyboardMarkup()
    
    # Sort users by total downloads
    sorted_users = sorted(
        users_data.items(),
        key=lambda x: x[1].get('total_downloads', 0),
        reverse=True
    )
    
    # Pagination
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_users = sorted_users[start_idx:end_idx]
    
    # Add user buttons
    for user_id_str, user_data in page_users:
        first_name = user_data.get('first_name', 'Unknown')
        username = user_data.get('username', 'No username')
        downloads = user_data.get('total_downloads', 0)
        status = user_data.get('status', 'active')
        
        status_emoji = "✅" if status == "active" else "🚫" if status == "banned" else "❓"
        button_text = f"{status_emoji} {first_name} (@{username}) - {downloads} downloads"
        
        keyboard.add(
            InlineKeyboardButton(
                button_text[:64],  # Telegram button text limit
                callback_data=f"user_details_{user_id_str}"
            )
        )
    
    # Pagination buttons
    pagination_row = []
    if page > 0:
        pagination_row.append(
            InlineKeyboardButton("⬅️ Previous", callback_data=f"user_list_page_{page-1}")
        )
    
    if end_idx < len(sorted_users):
        pagination_row.append(
            InlineKeyboardButton("➡️ Next", callback_data=f"user_list_page_{page+1}")
        )
    
    if pagination_row:
        keyboard.row(*pagination_row)
    
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main"))
    
    return keyboard

@bot.message_handler(commands=['start'])
def start_command(message: Message):
    """Handle /start command"""
    user = message.from_user
    if not user:
        return
        
    # Check if user is banned
    user_stats = data_manager.get_user_stats(user.id)
    if user_stats.get("status") == "banned":
        bot.send_message(
            message.chat.id,
            "🚫 **Account Suspended**\n\n"
            "Your account has been suspended by the administrator.\n"
            "Please contact support if you believe this is an error.",
            parse_mode='Markdown'
        )
        return
    
    data_manager.add_user(user.id, user.username, user.first_name)
    data_manager.log_command(user.id, 'start')
    
    welcome_text = f"""🎬 **Welcome to Professional Video Downloader!**

👋 **Hello {user.first_name}!**

I can help you download videos from 1000+ platforms including:
• YouTube • TikTok • Instagram • Twitter/X • Facebook
• Reddit • Vimeo • Dailymotion • And many more!

🚀 **How to use:**
• Just send me any video URL
• Use the menu buttons below for easy navigation
• Check your stats to see download history

⚡ **Features:**
• High-quality downloads
• Fast processing  
• Automatic format selection
• Download history tracking
• Professional admin panel

🎯 **Ready to start?** Send me a video link or use the buttons below!

---
💡 **Pro Tip:** Use "📊 My Stats" to track your downloads!"""

    bot.send_message(
        message.chat.id,
        welcome_text,
        reply_markup=create_main_menu(),
        parse_mode='Markdown'
    )

@bot.message_handler(func=lambda message: message.text and message.text == "📥 Download Video", content_types=['text'])
def download_request(message: Message):
    """Handle download video button request"""
    user = message.from_user
    if not user:
        return
        
    data_manager.add_user(user.id, user.username, user.first_name)
    data_manager.log_command(user.id, 'download_request')
    
    request_text = """📥 **Video Download Guide**

🎯 **How to download:**
1. Find the video you want on any platform
2. Copy the video URL/link
3. Send it to me here
4. Wait for processing (10-30 seconds)
5. Enjoy your downloaded video!

🌍 **Supported platforms:**
• YouTube (youtube.com/youtu.be)
• TikTok (tiktok.com)  
• Instagram (instagram.com)
• Twitter/X (twitter.com/x.com)
• Facebook (facebook.com)
• Reddit (reddit.com)
• Vimeo (vimeo.com)
• Dailymotion (dailymotion.com)
• And 1000+ more sites!

📋 **Example links:**
`https://youtube.com/watch?v=...`
`https://tiktok.com/@user/video/...`
`https://instagram.com/p/...`

⚡ **Just paste your video URL below and I'll handle the rest!**

---
💡 **Tip:** Make sure the video is public and not restricted."""

    bot.send_message(message.chat.id, request_text, parse_mode='Markdown')

@bot.message_handler(func=lambda message: message.text and message.text == "📊 My Stats", content_types=['text'])
def user_stats(message: Message):
    """Show user statistics"""
    user = message.from_user
    if not user:
        return
        
    data_manager.add_user(user.id, user.username, user.first_name)
    data_manager.log_command(user.id, 'stats')
    
    stats = data_manager.get_user_stats(user.id)
    if not stats:
        bot.send_message(message.chat.id, "❌ No stats found. Start by downloading a video!")
        return
    
    # Calculate stats
    join_date = stats.get('join_date', 'Unknown')[:10]
    total_downloads = stats.get('total_downloads', 0)
    commands_used = stats.get('commands_used', {})
    link_count = stats.get('link_count', 0)
    channel_joined = stats.get('channel_joined', False)
    
    # Platform distribution
    platform_stats = {}
    downloads = stats.get('downloads', [])
    for download in downloads:
        platform = download.get('platform', 'Unknown')
        platform_stats[platform] = platform_stats.get(platform, 0) + 1
    
    # Calculate total MB downloaded by user
    total_mb = stats.get('total_mb_downloaded', 0.0)
    
    stats_text = f"""📊 **Your Download Statistics**

👤 **Profile:**
• Name: {escape_markdown(user.first_name or 'Unknown')}
• Username: @{escape_markdown(user.username or 'None')}
• Join Date: {join_date}
• Channel Status: {'✅ Joined' if channel_joined else '❌ Not joined'}

📈 **Activity:**
• Total Downloads: {total_downloads}
• Total Data Downloaded: {total_mb:.1f} MB
• Links Sent: {link_count}
• Commands Used: {sum(commands_used.values())}"""
    
    if platform_stats:
        stats_text += f"\n\n🌍 **Platforms Used:**"
        for platform, count in sorted(platform_stats.items(), key=lambda x: x[1], reverse=True):
            stats_text += f"\n• {escape_markdown(platform)}: {count} videos"
    
    if downloads:
        stats_text += f"\n\n🎬 **Recent Downloads** (Last 5):"
        for download in downloads[-5:]:
            title = escape_markdown(download.get('title', 'Unknown')[:30])
            platform = escape_markdown(download.get('platform', 'Unknown'))
            timestamp = download.get('timestamp', '')[:10]
            stats_text += f"\n• {title}\\.\\.\\. \\({platform}\\) \\[{timestamp}\\]"
    
    stats_text += f"\n\n🎯 **Keep downloading to improve your stats!**"
    
    bot.send_message(message.chat.id, stats_text, parse_mode='Markdown')

@bot.message_handler(func=lambda message: message.text and message.text == "I AM BOSS", content_types=['text'])
def admin_panel_request(message: Message):
    """Handle admin panel access request"""
    user = message.from_user
    if not user:
        return
        
    data_manager.add_user(user.id, user.username, user.first_name)
    
    # Delete the "I AM BOSS" message immediately for security
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except:
        pass
    
    # Set user as waiting for PIN
    waiting_for_admin_pin[user.id] = True
    
    pin_msg = bot.send_message(
        message.chat.id,
        "🔐 **Admin Access Request**\n\n"
        "Please enter the admin PIN to continue:\n\n"
        "⚠️ **Security Notice:**\n"
        "• This message will self-destruct\n"
        "• Your PIN will be deleted immediately\n"
        "• Unauthorized access attempts are logged\n\n"
        "Enter your 6-digit PIN:",
        parse_mode='Markdown'
    )
    
    # Schedule deletion of PIN request message
    schedule_message_deletion(message.chat.id, pin_msg.message_id)

@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in waiting_for_admin_pin and message.text, content_types=['text'])
def handle_admin_pin_entry(message: Message):
    """Handle admin PIN entry"""
    user = message.from_user
    if not user or user.id not in waiting_for_admin_pin:
        return
    
    entered_pin = message.text.strip() if message.text else ""
    
    # Remove user from waiting list
    del waiting_for_admin_pin[user.id]
    
    # Delete PIN message immediately for security
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except:
        pass
    
    # Check PIN - Support both environment and admin settings PIN
    admin_pin_from_settings = data_manager.get_admin_setting("admin_pin", ADMIN_PIN)
    
    if entered_pin == ADMIN_PIN or entered_pin == admin_pin_from_settings:
        # Correct PIN - show admin panel
        data_manager.log_command(user.id, 'admin_access_granted')
        logger.info(f"Admin access granted to user {user.id} ({user.username})")
        show_admin_panel(message.chat.id, user.id)
    else:
        # Wrong PIN
        data_manager.log_command(user.id, 'admin_access_denied')
        logger.warning(f"Failed admin access attempt by user {user.id} ({user.username}) with PIN: {entered_pin}")
        
        error_msg = bot.send_message(
            message.chat.id,
            "❌ **Access Denied**\n\n"
            "Invalid admin PIN entered.\n\n"
            "⚠️ **Security Alert:**\n"
            "• This attempt has been logged\n"
            "• Contact the bot owner if you forgot the PIN\n"
            "• Repeated unauthorized attempts may result in restrictions",
            parse_mode='Markdown'
        )
        
        # Schedule deletion of error message
        schedule_message_deletion(message.chat.id, error_msg.message_id, 60)

def show_admin_panel(chat_id: int, user_id: int):
    """Display the admin panel with comprehensive bot statistics"""
    try:
        all_stats: Dict[str, Any] = data_manager.get_all_stats()
        bot_stats: Dict[str, Any] = all_stats.get("bot_stats") or {}
        users_data: Dict[str, Dict[str, Any]] = all_stats.get("users") or {}
        admin_settings: Dict[str, Any] = all_stats.get("admin_settings") or {}
        
        # Bot Statistics
        deletion_time = admin_settings.get('message_deletion_time', 300)
        deletion_status = "Enabled" if admin_settings.get('auto_delete_enabled', True) else "Disabled"
        
        admin_text = f"""🔥 ADMIN PANEL - BOT MASTER DASHBOARD 🔥

🤖 BOT STATISTICS:
• Total Users: {bot_stats.get('total_users', 0)}
• Total Downloads: {bot_stats.get('total_downloads', 0)}
• Bot Start Date: {bot_stats.get('start_date', 'Unknown')[:10]}
• Last Updated: {bot_stats.get('last_updated', 'Unknown')[:19]}

⚙️ ADMIN SETTINGS:
• Auto-Delete: {deletion_status}
• Deletion Time: {deletion_time // 60} minutes
• File Auto-Removal: {'✅ Enabled' if admin_settings.get('auto_removal_enabled', True) else '❌ Disabled'}
• Base Removal Time: {admin_settings.get('base_removal_time_minutes', 30)} minutes
• Pending Deletions: {len(pending_message_deletions)}
• Scheduled File Removals: {len(file_removal_manager.get_scheduled_removals())}

👥 USER ANALYTICS:
• Active Users: {len([u for u in users_data.values() if u.get('status') == 'active'])}
• Banned Users: {len([u for u in users_data.values() if u.get('status') == 'banned'])}
• Total Registered: {len(users_data)}

📊 TOP USERS BY DOWNLOADS:"""
        
        # Sort users by download count
        sorted_users = sorted(
            users_data.values(),
            key=lambda x: x.get('total_downloads', 0),
            reverse=True
        )[:5]  # Show only top 5 in main panel
        
        for i, user in enumerate(sorted_users, 1):
            username = user.get('username', 'No username')
            first_name = user.get('first_name', 'Unknown')
            downloads = user.get('total_downloads', 0)
            admin_text += f"\n{i}. {first_name} (@{username}) - {downloads} downloads"
        
        # Platform Statistics
        platform_stats = {}
        for user_data in users_data.values():
            for download in user_data.get('downloads', []):
                platform = download.get('platform', 'Unknown')
                platform_stats[platform] = platform_stats.get(platform, 0) + 1
        
        admin_text += f"\n\n🌐 TOP PLATFORMS:"
        for platform, count in sorted(platform_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
            admin_text += f"\n• {platform}: {count} downloads"
        
        # System Information
        admin_text += f"\n\n🖥️ SYSTEM INFO:"
        admin_text += f"\n• Downloads Directory: {DOWNLOADS_DIR}"
        admin_text += f"\n• Max File Size: {format_file_size(MAX_FILE_SIZE)}"
        
        # File count in downloads directory
        try:
            file_count = len(os.listdir(DOWNLOADS_DIR)) if os.path.exists(DOWNLOADS_DIR) else 0
            admin_text += f"\n• Files in Downloads: {file_count}"
        except:
            admin_text += f"\n• Files in Downloads: Error reading"
            
        admin_text += f"\n\n👑 You are the BOSS! Use buttons below for detailed actions. 👑"
        
        panel_msg = bot.send_message(
            chat_id, 
            admin_text,
            reply_markup=create_admin_settings_keyboard()
        )
        
        # Schedule deletion of admin panel
        schedule_message_deletion(chat_id, panel_msg.message_id)
        
    except Exception as e:
        logger.error(f"Admin panel error: {e}")
        bot.send_message(chat_id, f"❌ Error generating admin panel: {str(e)}")

@bot.message_handler(func=lambda message: message.text and message.text == "ℹ️ Help", content_types=['text'])
def help_command(message: Message):
    """Show help information"""
    user = message.from_user
    if not user:
        return
        
    data_manager.add_user(user.id, user.username, user.first_name)
    data_manager.log_command(user.id, 'help')
    
    help_text = """ℹ️ **Video Download Bot - Help Guide**

🎯 **Main Features:**
• Download videos from 1000+ platforms
• High-quality downloads optimized for Telegram
• Personal download statistics
• Professional admin panel (for bot owners)
• Automatic file management with time-based removal

🌍 **Supported Platforms:**
• YouTube
• TikTok
• Instagram (posts & stories)
• Twitter/X
• Facebook
• Reddit
• Dailymotion
• Vimeo
• And 1000+ more sites!

⚡ **Features:**
• High-quality downloads
• Fast processing
• Automatic format selection
• File size optimization for Telegram
• Download history tracking
• Personal statistics
• Smart file auto-removal system

🔧 **Commands:**
• Just send any video URL to download
• Use menu buttons for easy navigation
• Check "📊 My Stats" for your download history

📁 **File Management:**
• Files are automatically removed to save space
• Removal time is based on file size
• Smaller files = faster removal
• 200MB files stay for 30 minutes by default

❓ **Troubleshooting:**
• Make sure the video URL is correct
• Some private videos can't be downloaded
• Large files are automatically compressed
• Try again if download fails

💡 **Tips:**
• YouTube playlists are supported
• Age-restricted content may not work
• Quality depends on source video

Need more help? Just send me your video link and I'll do the rest! 🎬"""

    bot.send_message(message.chat.id, help_text, parse_mode='Markdown')

@bot.message_handler(func=lambda message: message.text and message.text == "🔗 Supported Sites", content_types=['text'])
def supported_sites(message: Message):
    """Show supported platforms"""
    user = message.from_user
    if not user:
        return
        
    data_manager.add_user(user.id, user.username, user.first_name)
    data_manager.log_command(user.id, 'supported_sites')
    
    sites_text = """🔗 **Supported Platforms & Sites**

🎥 **Video Platforms:**
• YouTube (youtube.com)
• TikTok (tiktok.com)
• Instagram (instagram.com)
• Twitter/X (twitter.com/x.com)
• Facebook (facebook.com)
• Reddit (reddit.com)
• Vimeo (vimeo.com)
• Dailymotion (dailymotion.com)
• Twitch Clips (twitch.tv)

📱 **Social Media:**
• TikTok videos & photos
• Instagram posts, reels, stories
• Twitter videos & GIFs
• Facebook videos
• Snapchat (public content)

🌍 **Regional Platforms:**
• Bilibili (China)
• Niconico (Japan)
• VK (Russia)
• Youku (China)
• And many more...

🎯 **Specialized Sites:**
• Educational platforms
• News websites with videos
• Sports streaming highlights
• Music platforms
• Gaming content

📊 **Total Supported:**
Over 1000+ websites supported through advanced extraction technology!

💡 **How it works:**
Just paste any video URL from these platforms and I'll automatically detect the platform and download the best quality available for Telegram sharing.

🚀 **Try it now:** Send me any video link to get started!"""

    bot.send_message(message.chat.id, sites_text, parse_mode='Markdown')

def is_video_url(text):
    """Check if text contains a video URL - with None safety"""
    if not text or not isinstance(text, str):
        return False
        
    video_domains = [
        'youtube.com', 'youtu.be', 'tiktok.com', 'instagram.com',
        'twitter.com', 'x.com', 'facebook.com', 'reddit.com',
        'vimeo.com', 'dailymotion.com', 'twitch.tv'
    ]
    
    text_lower = text.lower()
    return any(domain in text_lower for domain in video_domains) and ('http' in text_lower)

# Universal message handler for enforcement - MUST come before other text handlers
@bot.message_handler(func=lambda message: message.text and message.from_user, content_types=['text'])
def universal_message_enforcement(message: Message):
    """Universal enforcement handler - processes ALL text messages for banned words and link tracking"""
    user = message.from_user
    if not user:
        return
    
    # Check if user is banned
    user_stats = data_manager.get_user_stats(user.id)
    if user_stats.get("status") == "banned":
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except:
            pass
        bot.send_message(
            message.chat.id,
            "🚫 **Account Suspended**\n\n"
            "Your account has been suspended.\n"
            "Contact support if you believe this is an error.",
            parse_mode='Markdown'
        )
        return
    
    # Add/update user
    data_manager.add_user(user.id, user.username, user.first_name)
    
    # Check for banned words FIRST (before any other processing)
    if handle_banned_words_detection(message):
        # Message was deleted due to banned words, stop processing
        logger.info(f"Message from user {user.id} deleted due to banned words")
        return
    
    # Check if message contains a link and log it
    message_has_links = message.text and ('http' in message.text.lower() or 'www.' in message.text.lower())
    
    if message_has_links:
        # Extract URLs from message
        import re
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, message.text or "")
        
        for url in urls:
            # Log each link
            link_info = data_manager.log_link(user.id, url)
            logger.info(f"Link logged for user {user.id}: {data_manager._detect_platform(url)} - Count: {link_info['link_count']}")
            
            # Check if this is a video URL that needs download processing
            if is_video_url(url):
                # Check if channel join is required
                if link_info.get('needs_channel_join', False):
                    handle_channel_join_requirement(message, url, link_info)
                    return  # Stop processing, user needs to join channel first
                else:
                    # Process video download
                    handle_video_download(message, url)
                    return  # Stop processing after handling video
    else:
        # NEW: Auto-remove anonymous text messages (messages without links)
        # Only remove if it's not a command and not a specific interaction
        if (message.text and 
            not message.text.startswith('/') and 
            message.text not in ["🧹 Clear Chat", "📥 Download Video", "📊 My Stats", "ℹ️ Help", "🔗 Supported Sites", "I AM BOSS"] and
            user.id not in waiting_for_admin_pin and
            user.id not in waiting_for_banned_word and
            user.id not in waiting_for_promotion_channel and
            user.id not in waiting_for_help_channel and
            user.id not in waiting_for_new_pin and
            user.id not in waiting_for_admin_message):
            
            # Check if anonymous text removal is enabled
            if data_manager.get_admin_setting("anonymous_text_removal", True):
                try:
                    bot.delete_message(message.chat.id, message.message_id)
                    logger.info(f"Auto-deleted anonymous text from user {user.id}: {message.text[:50]}")
                    return
                except Exception as e:
                    logger.warning(f"Could not delete anonymous text message: {e}")
    
    # Check for specific button commands
    if message.text == "🧹 Clear Chat":
        clear_chat_request(message)
        return
    elif message.text == "📥 Download Video":
        download_request(message)
        return
    elif message.text == "📊 My Stats":
        user_stats_handler(message)
        return
    elif message.text == "ℹ️ Help":
        help_command(message)
        return
    elif message.text == "🔗 Supported Sites":
        supported_sites(message)
        return
    elif message.text == "I AM BOSS":
        admin_panel_request(message)
        return
    elif user.id in waiting_for_admin_pin:
        handle_admin_pin_entry(message)
        return
    elif user.id in waiting_for_banned_word:
        handle_banned_word_addition(message)
        return
    elif user.id in waiting_for_promotion_channel:
        handle_promotion_channel_input(message)
        return
    elif user.id in waiting_for_help_channel:
        handle_help_channel_input(message)
        return
    elif user.id in waiting_for_new_pin:
        handle_new_pin_input(message)
        return
    elif user.id in waiting_for_admin_message:
        handle_admin_message_input(message)
        return
    
    # If no specific handling, check if it looks like a video URL
    if is_video_url(message.text):
        # This is already handled above in link detection
        pass
    else:
        # General message - could be conversational
        # Just log the message activity (already done via add_user above)
        pass

def user_stats_handler(message: Message):
    """Wrapper for user stats to avoid name collision"""
    user_stats(message)

@bot.message_handler(func=lambda message: message.text and message.text == "🧹 Clear Chat", content_types=['text'])
def clear_chat_request(message: Message):
    """Handle clear chat request with confirmation"""
    user = message.from_user
    if not user:
        return
        
    data_manager.add_user(user.id, user.username, user.first_name)
    data_manager.log_command(user.id, 'clear_chat_request')
    
    # Create confirmation keyboard
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("✅ Yes, Clear All", callback_data="confirm_clear_chat"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_clear_chat")
    )
    
    bot.send_message(
        message.chat.id,
        "🧹 **Clear Chat Confirmation**\n\n"
        "⚠️ This will delete all messages in this chat including:\n"
        "• Download history\n"
        "• Bot responses\n"
        "• Your commands\n\n"
        "Are you sure you want to continue?",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

def show_channel_settings(call):
    """Show channel join settings with FIXED channel link updates"""
    admin_settings = data_manager.get_all_stats().get("admin_settings", {})
    required = admin_settings.get("channel_join_required", True)
    threshold = admin_settings.get("channel_join_after_links", 5)
    promotion_channel = admin_settings.get("promotion_channel", "https://t.me/follwnowo")
    help_channel = admin_settings.get("help_channel", "https://t.me/+enYm2HitF0BkNTZl")
    
    settings_text = f"📺 **Channel Join Settings**\n\n"
    settings_text += f"⚙️ **Status:** {'✅ Required' if required else '❌ Disabled'}\n"
    settings_text += f"🔢 **Link Threshold:** {threshold} links\n"
    settings_text += f"📢 **Promotion Channel:** `{promotion_channel}`\n"
    settings_text += f"💬 **Help Channel:** `{help_channel[:50]}{'...' if len(help_channel) > 50 else ''}`\n\n"
    settings_text += f"📈 **How it works:**\n"
    settings_text += f"Users must join channels after sending {threshold} video links.\n"
    settings_text += f"This helps grow our community while providing unlimited access.\n\n"
    settings_text += f"🔗 **Channel Management:**\n"
    settings_text += f"• Links are updated immediately when changed\n"
    settings_text += f"• Both public and private channels supported\n"
    settings_text += f"• Automatic membership verification when possible"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            f"{'🔴 Disable' if required else '🟢 Enable'} Channel Join",
            callback_data="toggle_channel_join"
        )
    )
    
    # Threshold setting buttons
    keyboard.add(InlineKeyboardButton("🔢 Set Threshold", callback_data="set_threshold_menu"))
    
    threshold_options = [3, 5, 10, 15, 20]
    for threshold_val in threshold_options:
        prefix = "✅ " if threshold_val == threshold else ""
        keyboard.add(
            InlineKeyboardButton(
                f"{prefix}{threshold_val} links",
                callback_data=f"set_link_threshold_{threshold_val}"
            )
        )
    
    # FIXED: Channel change buttons
    keyboard.add(
        InlineKeyboardButton("🔔 Change Promotion Channel", callback_data="change_promotion_channel"),
        InlineKeyboardButton("💬 Change Help Channel", callback_data="change_help_channel")
    )
    
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main"))
    
    bot.edit_message_text(
        settings_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

# NEW: Missing admin callback handler functions and user management

def show_user_list(call, page=0):
    """Show paginated user list for admin with management options"""
    all_stats = data_manager.get_all_stats()
    users_data = all_stats.get("users", {})
    
    if not users_data:
        bot.edit_message_text(
            "👥 **User List**\n\n❌ No users found.",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main")
            ),
            parse_mode='Markdown'
        )
        return
    
    keyboard = create_user_list_keyboard(users_data, page)
    
    # Count stats
    total_users = len(users_data)
    active_users = len([u for u in users_data.values() if u.get('status') == 'active'])
    banned_users = len([u for u in users_data.values() if u.get('status') == 'banned'])
    total_downloads = sum(u.get('total_downloads', 0) for u in users_data.values())
    
    list_text = f"👥 **User Management** (Page {page + 1})\n\n"
    list_text += f"📊 **Statistics:**\n"
    list_text += f"• Total Users: {total_users}\n"
    list_text += f"• Active Users: {active_users}\n"
    list_text += f"• Banned Users: {banned_users}\n"
    list_text += f"• Total Downloads: {total_downloads}\n\n"
    list_text += f"👆 **Click on any user below for detailed management options:**"
    
    bot.edit_message_text(
        list_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

def show_user_details(call, user_id_str):
    """Show detailed information for a specific user with management options"""
    all_stats = data_manager.get_all_stats()
    users_data = all_stats.get("users", {})
    
    user_data = users_data.get(user_id_str)
    if not user_data:
        bot.edit_message_text(
            "❌ **User not found**",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='Markdown'
        )
        return
    
    # Format user details
    username = user_data.get('username', 'No username')
    first_name = user_data.get('first_name', 'Unknown')
    join_date = user_data.get('join_date', 'Unknown')[:10]
    last_activity = user_data.get('last_activity', 'Unknown')[:10]
    total_downloads = user_data.get('total_downloads', 0)
    link_count = user_data.get('link_count', 0)
    channel_joined = user_data.get('channel_joined', False)
    status = user_data.get('status', 'active')
    ban_date = user_data.get('ban_date', '')[:10] if user_data.get('ban_date') else ''
    
    user_details_text = f"👤 **User Details**\n\n"
    user_details_text += f"**Basic Info:**\n"
    user_details_text += f"• Name: {escape_markdown(first_name)}\n"
    user_details_text += f"• Username: @{escape_markdown(username)}\n"
    user_details_text += f"• User ID: `{user_id_str}`\n"
    user_details_text += f"• Status: {escape_markdown(status.title())}\n"
    if ban_date:
        user_details_text += f"• Ban Date: {ban_date}\n"
    user_details_text += f"\n"
    
    user_details_text += f"**Activity:**\n"
    user_details_text += f"• Join Date: {join_date}\n"
    user_details_text += f"• Last Activity: {last_activity}\n"
    user_details_text += f"• Total Downloads: {total_downloads}\n"
    user_details_text += f"• Links Sent: {link_count}\n"
    user_details_text += f"• Channel Joined: {'✅ Yes' if channel_joined else '❌ No'}\n\n"
    
    # Recent downloads
    downloads = user_data.get('downloads', [])
    user_details_text += f"**Recent Downloads** (Last 5):\n"
    if downloads:
        for download in downloads[-5:]:
            title = escape_markdown(download.get('title', 'Unknown')[:30])
            platform = escape_markdown(download.get('platform', 'Unknown'))
            timestamp = download.get('timestamp', '')[:10]
            user_details_text += f"• {title}\\.\\.\\. \\({platform}\\) \\[{timestamp}\\]\n"
    else:
        user_details_text += f"• No downloads yet\n"
    
    # Command usage
    commands = user_data.get('commands_used', {})
    if commands:
        user_details_text += f"\n**Command Usage:**\n"
        for cmd, count in sorted(commands.items(), key=lambda x: x[1], reverse=True)[:3]:
            user_details_text += f"• {escape_markdown(cmd)}: {count} times\n"
    
    # Platform distribution
    platform_stats = {}
    for download in downloads:
        platform = download.get('platform', 'Unknown')
        platform_stats[platform] = platform_stats.get(platform, 0) + 1
    
    if platform_stats:
        user_details_text += f"\n**Platform Usage:**\n"
        for platform, count in sorted(platform_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
            user_details_text += f"• {escape_markdown(platform)}: {count} downloads\n"
    
    # Management keyboard
    keyboard = InlineKeyboardMarkup()
    
    # User management actions
    if status == "active":
        keyboard.add(
            InlineKeyboardButton("🚫 Ban User", callback_data=f"ban_user_{user_id_str}"),
            InlineKeyboardButton("💬 Send Message", callback_data=f"message_user_{user_id_str}")
        )
    else:
        keyboard.add(
            InlineKeyboardButton("✅ Unban User", callback_data=f"unban_user_{user_id_str}"),
            InlineKeyboardButton("💬 Send Message", callback_data=f"message_user_{user_id_str}")
        )
    
    keyboard.add(
        InlineKeyboardButton("🗑️ Delete Account", callback_data=f"delete_user_{user_id_str}"),
        InlineKeyboardButton("📊 Reset Stats", callback_data=f"reset_user_stats_{user_id_str}")
    )
    
    keyboard.add(InlineKeyboardButton("🔙 Back to User List", callback_data="admin_user_list"))
    keyboard.add(InlineKeyboardButton("🏠 Main Admin Panel", callback_data="admin_main"))
    
    bot.edit_message_text(
        user_details_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

# NEW: File Management for Admin
def show_file_management(call):
    """Show file management interface for admin"""
    scheduled_removals = file_removal_manager.get_scheduled_removals()
    admin_settings = data_manager.get_all_stats().get("admin_settings", {})
    
    auto_removal_enabled = admin_settings.get("auto_removal_enabled", True)
    base_time = admin_settings.get("base_removal_time_minutes", 30)
    
    try:
        file_count = len(os.listdir(DOWNLOADS_DIR)) if os.path.exists(DOWNLOADS_DIR) else 0
        
        # Calculate total disk usage
        total_size = 0
        if os.path.exists(DOWNLOADS_DIR):
            for file in os.listdir(DOWNLOADS_DIR):
                file_path = os.path.join(DOWNLOADS_DIR, file)
                if os.path.isfile(file_path):
                    total_size += os.path.getsize(file_path)
    except:
        file_count = 0
        total_size = 0
    
    management_text = f"🗂️ **File Management Dashboard**\n\n"
    management_text += f"📁 **Storage Status:**\n"
    management_text += f"• Total Files: {file_count}\n"
    management_text += f"• Total Size: {format_file_size(total_size)}\n"
    management_text += f"• Downloads Directory: `{DOWNLOADS_DIR}`\n\n"
    
    management_text += f"⚙️ **Auto-Removal Settings:**\n"
    management_text += f"• Status: {'✅ Enabled' if auto_removal_enabled else '❌ Disabled'}\n"
    management_text += f"• Base Time (200MB): {base_time} minutes\n"
    management_text += f"• Scheduled Removals: {len(scheduled_removals)}\n\n"
    
    if scheduled_removals:
        management_text += f"⏰ **Upcoming Removals:**\n"
        for file_path, info in list(scheduled_removals.items())[:5]:
            file_name = os.path.basename(file_path)[:30]
            size_mb = info['file_size'] / (1024 * 1024)
            removal_minutes = info['removal_minutes']
            management_text += f"• {file_name}: {size_mb:.1f}MB in {removal_minutes}min\n"
        
        if len(scheduled_removals) > 5:
            management_text += f"• ... and {len(scheduled_removals) - 5} more\n"
    else:
        management_text += f"⏰ **No scheduled removals**\n"
    
    management_text += f"\n💡 **File Management:**\n"
    management_text += f"Files are automatically removed based on size to save space.\n"
    management_text += f"Smaller files are removed faster than larger files."
    
    keyboard = InlineKeyboardMarkup()
    
    keyboard.add(
        InlineKeyboardButton(
            f"{'🔴 Disable' if auto_removal_enabled else '🟢 Enable'} Auto-Removal",
            callback_data="toggle_auto_removal"
        )
    )
    
    # Base time options
    time_options = [15, 30, 45, 60, 120]
    for time_val in time_options:
        prefix = "✅ " if time_val == base_time else ""
        keyboard.add(
            InlineKeyboardButton(
                f"{prefix}{time_val} min base time",
                callback_data=f"set_base_removal_time_{time_val}"
            )
        )
    
    keyboard.add(
        InlineKeyboardButton("🧹 Clean All Files Now", callback_data="clean_all_files"),
        InlineKeyboardButton("🔄 Refresh", callback_data="admin_file_management")
    )
    
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main"))
    
    bot.edit_message_text(
        management_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    """Handle all inline keyboard callbacks with comprehensive admin features"""
    user = call.from_user
    if not user:
        return
    
    # Safety check for call.message
    if not call.message:
        logger.warning(f"Callback query {call.data} received without message object")
        try:
            bot.answer_callback_query(call.id, "❌ Error: Invalid callback")
        except:
            pass
        return
    
    try:
        # Admin panel callbacks
        if call.data == "admin_main":
            show_admin_dashboard_callback(call)
            
        elif call.data == "admin_refresh":
            show_admin_dashboard_callback(call)
            
        elif call.data == "admin_msg_settings":
            show_message_deletion_settings(call)
            
        elif call.data == "admin_user_list":
            show_user_list(call, page=0)
            
        elif call.data.startswith("user_list_page_"):
            page = int(call.data.split("_")[-1])
            show_user_list(call, page=page)
            
        elif call.data.startswith("user_details_"):
            user_id_str = call.data.replace("user_details_", "")
            show_user_details(call, user_id_str)
            
        elif call.data.startswith("set_deletion_time_"):
            deletion_time = int(call.data.replace("set_deletion_time_", ""))
            update_deletion_time_setting(call, deletion_time)
            
        elif call.data == "admin_banned_words":
            show_banned_words_settings(call)
            
        elif call.data == "admin_channel_settings":
            show_channel_settings(call)
            
        elif call.data == "admin_link_analytics":
            show_link_analytics(call)
            
        elif call.data == "admin_bot_settings":
            show_bot_settings(call)
        
        elif call.data == "admin_file_management":
            show_file_management(call)
            
        elif call.data.startswith("remove_banned_word_"):
            word = call.data.replace("remove_banned_word_", "")
            if data_manager.remove_banned_word(word):
                show_banned_words_settings(call)
            
        elif call.data == "add_banned_word":
            handle_add_banned_word_request(call)
            
        elif call.data == "toggle_banned_words":
            current_enabled = data_manager.get_admin_setting("banned_words_enabled", True)
            data_manager.update_admin_setting("banned_words_enabled", not current_enabled)
            show_banned_words_settings(call)
            
        elif call.data == "toggle_channel_join":
            current_enabled = data_manager.get_admin_setting("channel_join_required", True)
            data_manager.update_admin_setting("channel_join_required", not current_enabled)
            show_channel_settings(call)
            
        elif call.data.startswith("set_link_threshold_"):
            threshold = int(call.data.replace("set_link_threshold_", ""))
            data_manager.update_admin_setting("channel_join_after_links", threshold)
            show_channel_settings(call)
            
        elif call.data == "verify_channel_join":
            handle_channel_join_verification(call)
        
        # NEW: Channel management callbacks
        elif call.data == "change_promotion_channel":
            handle_promotion_channel_change_request(call)
            
        elif call.data == "change_help_channel":
            handle_help_channel_change_request(call)
        
        # NEW: User management callbacks
        elif call.data.startswith("ban_user_"):
            user_id_str = call.data.replace("ban_user_", "")
            if data_manager.ban_user_account(int(user_id_str)):
                bot.answer_callback_query(call.id, "✅ User banned successfully")
                show_user_details(call, user_id_str)
            else:
                bot.answer_callback_query(call.id, "❌ Failed to ban user")
                
        elif call.data.startswith("unban_user_"):
            user_id_str = call.data.replace("unban_user_", "")
            if data_manager.unban_user_account(int(user_id_str)):
                bot.answer_callback_query(call.id, "✅ User unbanned successfully")
                show_user_details(call, user_id_str)
            else:
                bot.answer_callback_query(call.id, "❌ Failed to unban user")
                
        elif call.data.startswith("delete_user_"):
            user_id_str = call.data.replace("delete_user_", "")
            if data_manager.remove_user_account(int(user_id_str)):
                bot.answer_callback_query(call.id, "✅ User account deleted")
                show_user_list(call, page=0)
            else:
                bot.answer_callback_query(call.id, "❌ Failed to delete user")
        
        # NEW: File management callbacks
        elif call.data == "toggle_auto_removal":
            current_enabled = data_manager.get_admin_setting("auto_removal_enabled", True)
            data_manager.update_admin_setting("auto_removal_enabled", not current_enabled)
            show_file_management(call)
            
        elif call.data.startswith("set_base_removal_time_"):
            time_minutes = int(call.data.replace("set_base_removal_time_", ""))
            data_manager.update_admin_setting("base_removal_time_minutes", time_minutes)
            show_file_management(call)
            
        elif call.data == "clean_all_files":
            # Clean all files in downloads directory
            try:
                count = 0
                if os.path.exists(DOWNLOADS_DIR):
                    for file in os.listdir(DOWNLOADS_DIR):
                        file_path = os.path.join(DOWNLOADS_DIR, file)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            count += 1
                            
                # Clear all scheduled removals
                file_removal_manager.scheduled_removals.clear()
                
                bot.answer_callback_query(call.id, f"✅ Cleaned {count} files")
                show_file_management(call)
            except Exception as e:
                bot.answer_callback_query(call.id, f"❌ Error: {str(e)}")
        
        elif call.data == "admin_change_pin":
            handle_admin_pin_change_request(call)
        
        elif call.data == "admin_remove_all_downloads":
            handle_remove_all_downloads_request(call)
            
        elif call.data == "confirm_remove_all_downloads":
            # Actually remove all downloads
            try:
                count = 0
                total_size_removed = 0
                if os.path.exists(DOWNLOADS_DIR):
                    for file in os.listdir(DOWNLOADS_DIR):
                        file_path = os.path.join(DOWNLOADS_DIR, file)
                        if os.path.isfile(file_path):
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            count += 1
                            total_size_removed += file_size
                
                # Clear all scheduled removals
                file_removal_manager.scheduled_removals.clear()
                
                bot.edit_message_text(
                    f"✅ **All Downloads Removed Successfully!**\n\n"
                    f"📊 **Removal Summary:**\n"
                    f"• Files removed: {count}\n"
                    f"• Space freed: {format_file_size(total_size_removed)}\n"
                    f"• Scheduled removals cancelled: All\n\n"
                    f"🧹 Downloads directory is now clean.\n"
                    f"Returning to file management in 3 seconds...",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='Markdown'
                )
                
                def return_to_file_management():
                    try:
                        show_file_management(call)
                    except Exception as e:
                        logger.error(f"Error returning to file management: {e}")
                
                threading.Timer(3.0, return_to_file_management).start()
                logger.info(f"Admin {call.from_user.id} removed all downloads: {count} files, {format_file_size(total_size_removed)}")
                
            except Exception as e:
                bot.edit_message_text(
                    f"❌ **Error Removing Downloads**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Some files may not have been removed.",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='Markdown'
                )
                logger.error(f"Error removing all downloads: {e}")
        
        # Clear chat callbacks
        elif call.data == "confirm_clear_chat":
            clear_chat_confirmed(call)
            
        elif call.data == "cancel_clear_chat":
            bot.edit_message_text(
                "❌ **Chat clear cancelled**\n\nYour chat history remains intact.",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            
        # Answer callback to remove loading state
        try:
            bot.answer_callback_query(call.id)
        except Exception as callback_error:
            logger.warning(f"Could not answer callback query: {callback_error}")
        
    except Exception as e:
        logger.error(f"Callback query error: {e}")
        try:
            bot.answer_callback_query(call.id, "❌ An error occurred")
        except Exception as callback_error:
            logger.warning(f"Could not answer callback query during error handling: {callback_error}")

def show_admin_dashboard_callback(call):
    """Show admin dashboard for callback - same as show_admin_panel"""
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    
    user = call.from_user
    if not user:
        return
        
    show_admin_panel(call.message.chat.id, user.id)

def show_message_deletion_settings(call):
    """Show message deletion time settings"""
    all_stats = data_manager.get_all_stats()
    admin_settings = all_stats.get("admin_settings", {})
    current_time = admin_settings.get("message_deletion_time", 300)
    auto_delete_enabled = admin_settings.get("auto_delete_enabled", True)
    
    settings_text = f"""⚙️ **Message Deletion Settings**

🔧 **Current Configuration:**
• Auto-Delete: {'✅ Enabled' if auto_delete_enabled else '❌ Disabled'}
• Deletion Time: {current_time // 60} minutes ({current_time} seconds)
• Pending Deletions: {len(pending_message_deletions)}

📝 **What gets auto-deleted:**
• "I AM BOSS" requests
• PIN entry messages
• Admin access errors
• This settings panel

⏰ **Choose new deletion time:**"""
    
    bot.edit_message_text(
        settings_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=create_message_deletion_settings_keyboard(),
        parse_mode='Markdown'
    )

def clear_chat_confirmed(call):
    """Handle confirmed chat clearing"""
    try:
        chat_id = call.message.chat.id
        
        # Delete all recent messages in the chat
        message_count = 0
        errors = 0
        
        # Try to delete the last 100 messages
        for i in range(1, 101):
            try:
                bot.delete_message(chat_id, call.message.message_id - i)
                message_count += 1
                time.sleep(0.1)  # Small delay to avoid rate limiting
            except Exception:
                errors += 1
                if errors > 20:  # Stop if too many errors
                    break
        
        # Delete the confirmation message itself
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        # Send completion message
        completion_msg = bot.send_message(
            chat_id,
            f"🧹 **Chat Cleared Successfully!**\n\n"
            f"✅ Deleted {message_count} messages\n"
            f"🆕 Chat history has been cleared\n\n"
            f"You can start fresh now! 🚀",
            reply_markup=create_main_menu(),
            parse_mode='Markdown'
        )
        
        # Schedule deletion of this completion message
        schedule_message_deletion(chat_id, completion_msg.message_id, 30)  # Delete in 30 seconds
        
    except Exception as e:
        logger.error(f"Error clearing chat: {e}")

# NEW: Missing admin functions implementations

def handle_channel_join_requirement(message, url, link_info):
    """Handle channel join requirement before allowing download"""
    user = message.from_user
    admin_settings = data_manager.get_all_stats().get("admin_settings", {})
    promotion_channel = admin_settings.get("promotion_channel", "https://t.me/follwnowo")
    help_channel = admin_settings.get("help_channel", "https://t.me/+enYm2HitF0BkNTZl")
    
    # Store the URL temporarily for after channel join
    user_pending_downloads[user.id] = url
    
    join_message = f"🔒 **Channel Join Required**\n\n"
    join_message += f"📈 You've sent {link_info['link_count']} links!\n"
    join_message += f"To continue downloading, please join our channels:\n\n"
    join_message += f"🎯 **Benefits of joining:**\n"
    join_message += f"• Unlimited downloads\n"
    join_message += f"• Latest updates\n"
    join_message += f"• Community support\n"
    join_message += f"• Priority processing\n\n"
    join_message += f"📋 **What to do:**\n"
    join_message += f"1. Join both channels below\n"
    join_message += f"2. Click 'I Joined' to verify\n"
    join_message += f"3. Continue with unlimited downloads!"
    
    keyboard = create_channel_join_keyboard(promotion_channel, help_channel)
    
    # Log the enforcement action
    logger.info(f"Channel join required for user {user.id} after {link_info['link_count']} links")
    
    bot.send_message(
        message.chat.id,
        join_message,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

def handle_video_download(message, url):
    """Handle video download processing with auto-removal scheduling"""
    user = message.from_user
    
    # Send initial processing message
    processing_msg = bot.send_message(
        message.chat.id,
        "🔄 **Processing your video...**\n\n"
        "📋 Analyzing URL...\n"
        "⏳ This may take 10-30 seconds depending on video size.\n\n"
        "Please wait...",
        parse_mode='Markdown'
    )
    
    try:
        # Get video info first
        bot.edit_message_text(
            "🔍 **Getting video information...**\n\n"
            "📋 Extracting metadata...\n"
            "⏳ Please wait...",
            message.chat.id,
            processing_msg.message_id,
            parse_mode='Markdown'
        )
        
        video_info = downloader.get_video_info(url)
        
        if not video_info:
            bot.edit_message_text(
                "❌ **Error: Could not extract video information**\n\n"
                "Please check if:\n"
                "• The URL is correct\n"
                "• The video is public\n"
                "• The platform is supported\n\n"
                "Try again with a different URL.",
                message.chat.id,
                processing_msg.message_id,
                parse_mode='Markdown'
            )
            return
        
        # Show video info
        title_text = video_info['title'][:50]
        uploader_text = video_info['uploader']
        platform_text = video_info['platform']
        
        info_text = f"📺 **Video Found!**\n\n"
        info_text += f"🎬 **Title:** {escape_markdown(title_text)}{'...' if len(video_info['title']) > 50 else ''}\n"
        info_text += f"👤 **Uploader:** {escape_markdown(uploader_text)}\n"
        info_text += f"⏱️ **Duration:** {format_duration(video_info['duration'])}\n"
        info_text += f"🌍 **Platform:** {escape_markdown(platform_text)}\n"
        info_text += f"📊 **Formats Available:** {video_info['formats_available']}\n\n"
        info_text += f"⬇️ **Starting download...**"
        
        bot.edit_message_text(
            info_text,
            message.chat.id,
            processing_msg.message_id,
            parse_mode='Markdown'
        )
        
        # Download video
        time.sleep(1)  # Brief pause for user to read info
        
        bot.edit_message_text(
            info_text + "\n\n🔄 **Downloading video file...**",
            message.chat.id,
            processing_msg.message_id,
            parse_mode='Markdown'
        )
        
        result = downloader.download_video(url, user.id)
        
        if result['success']:
            file_path = result['file_path']
            file_size = result['file_size']
            removal_info = result.get('removal_info', {})
            
            # Update message to uploading
            bot.edit_message_text(
                info_text + "\n\n📤 **Uploading to Telegram...**",
                message.chat.id,
                processing_msg.message_id,
                parse_mode='Markdown'
            )
            
            # Prepare caption with removal info
            caption_text = f"🎬 **{escape_markdown(result['title'])}**\n\n"
            caption_text += f"📊 **Stats:** {format_file_size(file_size)} • {format_duration(result['duration'])}\n"
            caption_text += f"🌍 **Source:** {escape_markdown(result['platform'])}\n"
            caption_text += f"⬇️ **Downloaded by:** @{escape_markdown(user.username or 'Unknown')}\n\n"
            
            # Add removal info to caption
            if removal_info.get('scheduled') and 'removal_minutes' in removal_info:
                removal_minutes = removal_info['removal_minutes']
                caption_text += f"⏰ **Auto-removal:** This file will be removed in {removal_minutes} minutes to save space."
            
            # Send video file
            with open(file_path, 'rb') as video_file:
                bot.send_video(
                    message.chat.id,
                    video_file,
                    caption=caption_text,
                    parse_mode='Markdown'
                )
            
            # Log the download with file size
            file_size_mb = result.get('file_size', 0) / (1024 * 1024) if result.get('file_size') else 0.0
            data_manager.log_download(user.id, url, result['title'], result['platform'], file_size_mb)
            
            # Update success message with removal info
            success_text = f"✅ **Download Complete!**\n\n"
            success_text += f"📂 **File:** {escape_markdown(result['title'][:30])}{'...' if len(result['title']) > 30 else ''}\n"
            success_text += f"📊 **Size:** {format_file_size(file_size)}\n"
            success_text += f"⏱️ **Duration:** {format_duration(result['duration'])}\n"
            success_text += f"🌍 **Platform:** {escape_markdown(result['platform'])}\n\n"
            
            if removal_info.get('scheduled') and 'removal_minutes' in removal_info:
                removal_minutes = removal_info['removal_minutes']
                success_text += f"⏰ **File Management:** This video will be automatically removed from our servers in {removal_minutes} minutes to save space. Your download is complete and safe!\n\n"
            
            success_text += f"🎉 Video uploaded successfully!\n"
            success_text += f"Send another link to download more videos! 🎬"
            
            bot.edit_message_text(
                success_text,
                message.chat.id,
                processing_msg.message_id,
                parse_mode='Markdown'
            )
            
        else:
            # Download failed
            error_msg = result.get('error', 'Unknown error')
            bot.edit_message_text(
                f"❌ **Download Failed**\n\n"
                f"📋 **Error:** {escape_markdown(error_msg)}\n\n"
                f"🔧 **Try these solutions:**\n"
                f"• Check if the video URL is correct\n"
                f"• Make sure the video is public\n"
                f"• Try a different video\n"
                f"• Contact support if the problem persists\n\n"
                f"Send another link to try again! 🔄",
                message.chat.id,
                processing_msg.message_id,
                parse_mode='Markdown'
            )
            
    except Exception as e:
        logger.error(f"Video download error: {e}")
        bot.edit_message_text(
            f"❌ **Download Error**\n\n"
            f"An unexpected error occurred:\n`{escape_markdown(str(e))}`\n\n"
            f"Please try again or contact support if the issue persists.",
            message.chat.id,
            processing_msg.message_id,
            parse_mode='Markdown'
        )

def handle_channel_join_verification(call):
    """Handle channel join verification with improved checking"""
    user = call.from_user
    if not user:
        return
    
    admin_settings = data_manager.get_all_stats().get("admin_settings", {})
    promotion_channel = admin_settings.get("promotion_channel", "https://t.me/follwnowo")
    help_channel = admin_settings.get("help_channel", "https://t.me/+enYm2HitF0BkNTZl")
    
    # Extract channel usernames from URLs
    promo_username = promotion_channel.replace('https://t.me/', '').replace('@', '')
    help_username = help_channel.replace('https://t.me/', '').replace('@', '')
    
    # Check membership in both channels
    promo_joined = check_channel_membership(user.id, promo_username)
    help_joined = check_channel_membership(user.id, help_username)
    
    # For private channels (starting with +), we can't verify automatically
    promo_is_private = promo_username.startswith('+')
    help_is_private = help_username.startswith('+')
    
    if (promo_joined or promo_is_private) and (help_joined or help_is_private):
        # User joined or channels are private - mark as joined
        data_manager.update_channel_join_status(user.id, True)
        
        # Get pending download URL
        pending_url = user_pending_downloads.get(user.id)
        
        if pending_url:
            del user_pending_downloads[user.id]
            
            bot.edit_message_text(
                "✅ **Channel Join Verified!**\n\n"
                "🎉 Thank you for joining our channels!\n"
                "🚀 You now have unlimited access to downloads.\n\n"
                "⬇️ **Processing your video now...**",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            
            # Process the pending download
            handle_video_download_from_callback(call, pending_url)
        else:
            bot.edit_message_text(
                "✅ **Channel Join Verified!**\n\n"
                "🎉 You now have unlimited access to downloads!\n"
                "📥 Send any video link to start downloading.",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
    else:
        # User hasn't joined yet
        missed_channels = []
        if not promo_joined and not promo_is_private:
            missed_channels.append("Promotion Channel")
        if not help_joined and not help_is_private:
            missed_channels.append("Help Channel")
        
        if missed_channels:
            bot.answer_callback_query(
                call.id,
                f"❌ Please join: {', '.join(missed_channels)}",
                show_alert=True
            )
        else:
            # Both channels are private, assume user joined
            data_manager.update_channel_join_status(user.id, True)
            bot.answer_callback_query(call.id, "✅ Verified! Processing download...")

def handle_video_download_from_callback(call, url):
    """Handle video download from callback (used after channel verification)"""
    # Create a mock message object for download processing
    class MockMessage:
        def __init__(self, call):
            self.chat = call.message.chat
            self.from_user = call.from_user
            self.message_id = call.message.message_id
    
    mock_message = MockMessage(call)
    handle_video_download(mock_message, url)

# NEW: Additional missing functions for complete admin functionality

def handle_promotion_channel_change_request(call):
    """Handle request to change promotion channel"""
    user = call.from_user
    if not user:
        return
    
    waiting_for_promotion_channel[user.id] = call.message.message_id
    
    bot.edit_message_text(
        "📢 **Change Promotion Channel**\n\n"
        "Please send the new promotion channel URL.\n\n"
        "**Supported formats:**\n"
        "• `https://t.me/channelname`\n"
        "• `https://t.me/+invitelink` (for private channels)\n"
        "• `@channelname`\n\n"
        "**Example:** `https://t.me/yourchannel`\n\n"
        "Send the new channel URL:",
        call.message.chat.id,
        call.message.message_id,
        parse_mode='Markdown'
    )

def handle_help_channel_change_request(call):
    """Handle request to change help channel"""
    user = call.from_user
    if not user:
        return
    
    waiting_for_help_channel[user.id] = call.message.message_id
    
    bot.edit_message_text(
        "💬 **Change Help Channel**\n\n"
        "Please send the new help channel URL.\n\n"
        "**Supported formats:**\n"
        "• `https://t.me/channelname`\n"
        "• `https://t.me/+invitelink` (for private channels)\n"
        "• `@channelname`\n\n"
        "**Example:** `https://t.me/yourhelpchannel`\n\n"
        "Send the new channel URL:",
        call.message.chat.id,
        call.message.message_id,
        parse_mode='Markdown'
    )

@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in waiting_for_promotion_channel and message.text, content_types=['text'])
def handle_promotion_channel_input(message):
    """Handle promotion channel input"""
    user = message.from_user
    if not user or user.id not in waiting_for_promotion_channel:
        return
    
    new_channel = message.text.strip() if message.text else ""
    original_message_id = waiting_for_promotion_channel[user.id]
    
    del waiting_for_promotion_channel[user.id]
    
    # Delete input message
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except:
        pass
    
    # Validate channel URL
    if new_channel and (new_channel.startswith('https://t.me/') or new_channel.startswith('@')):
        data_manager.update_admin_setting("promotion_channel", new_channel)
        
        bot.edit_message_text(
            f"✅ **Promotion Channel Updated!**\n\n"
            f"📢 **New Channel:** `{new_channel}`\n\n"
            f"Changes take effect immediately for new users.\n"
            f"Returning to settings in 3 seconds...",
            message.chat.id,
            original_message_id,
            parse_mode='Markdown'
        )
        
        def return_to_settings():
            try:
                class MockCall:
                    def __init__(self):
                        self.message = message
                        self.from_user = user
                mock_call = MockCall()
                mock_call.message.message_id = original_message_id
                show_channel_settings(mock_call)
            except Exception as e:
                logger.error(f"Error returning to channel settings: {e}")
        
        threading.Timer(3.0, return_to_settings).start()
        logger.info(f"Admin {user.id} changed promotion channel to: {new_channel}")
    else:
        bot.edit_message_text(
            "❌ **Invalid Channel URL**\n\n"
            "Please use a valid format:\n"
            "• `https://t.me/channelname`\n"
            "• `https://t.me/+invitelink`\n"
            "• `@channelname`\n\n"
            "Please try again.",
            message.chat.id,
            original_message_id,
            parse_mode='Markdown'
        )

@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in waiting_for_help_channel and message.text, content_types=['text'])
def handle_help_channel_input(message):
    """Handle help channel input"""
    user = message.from_user
    if not user or user.id not in waiting_for_help_channel:
        return
    
    new_channel = message.text.strip() if message.text else ""
    original_message_id = waiting_for_help_channel[user.id]
    
    del waiting_for_help_channel[user.id]
    
    # Delete input message
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except:
        pass
    
    if new_channel and (new_channel.startswith('https://t.me/') or new_channel.startswith('@')):
        data_manager.update_admin_setting("help_channel", new_channel)
        
        bot.edit_message_text(
            f"✅ **Help Channel Updated!**\n\n"
            f"💬 **New Channel:** `{new_channel}`\n\n"
            f"Changes take effect immediately.\n"
            f"Returning to settings in 3 seconds...",
            message.chat.id,
            original_message_id,
            parse_mode='Markdown'
        )
        
        def return_to_settings():
            try:
                class MockCall:
                    def __init__(self):
                        self.message = message
                        self.from_user = user
                mock_call = MockCall()
                mock_call.message.message_id = original_message_id
                show_channel_settings(mock_call)
            except Exception as e:
                logger.error(f"Error returning to channel settings: {e}")
        
        threading.Timer(3.0, return_to_settings).start()
        logger.info(f"Admin {user.id} changed help channel to: {new_channel}")
    else:
        bot.send_message(message.chat.id, "❌ Invalid channel URL. Please try again.")

def handle_add_banned_word_request(call):
    """Handle request to add a banned word"""
    user = call.from_user
    if not user:
        return
    
    waiting_for_banned_word[user.id] = call.message.message_id
    
    bot.edit_message_text(
        "🚫 **Add Banned Word**\n\n"
        "Please send the word or phrase you want to ban.\n\n"
        "⚠️ **Important:**\n"
        "• Messages containing this word will be auto-deleted\n"
        "• Case-insensitive matching\n"
        "• Use responsibly\n\n"
        "Send the word to ban:",
        call.message.chat.id,
        call.message.message_id,
        parse_mode='Markdown'
    )

@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in waiting_for_banned_word and message.text, content_types=['text'])
def handle_banned_word_addition(message):
    """Handle banned word addition"""
    user = message.from_user
    if not user or user.id not in waiting_for_banned_word:
        return
    
    new_word = message.text.strip() if message.text else ""
    original_message_id = waiting_for_banned_word[user.id]
    
    del waiting_for_banned_word[user.id]
    
    # Delete input message
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except:
        pass
    
    if new_word:
        if data_manager.add_banned_word(new_word):
            try:
                bot.edit_message_text(
                    f"✅ **Banned Word Added!**\n\n"
                    f"🚫 **Word:** `{new_word}`\n\n"
                    f"Messages containing this word will now be auto-deleted.\n"
                    f"Returning to settings in 3 seconds...",
                    message.chat.id,
                    original_message_id,
                    parse_mode='Markdown'
                )
                
                # Schedule return to settings
                def return_to_settings():
                    try:
                        # Create a mock call object
                        class MockCall:
                            def __init__(self):
                                self.message = message
                                self.from_user = user
                        
                        mock_call = MockCall()
                        mock_call.message.message_id = original_message_id
                        show_banned_words_settings(mock_call)
                    except Exception as e:
                        logger.error(f"Error returning to banned words settings: {e}")
                
                threading.Timer(3.0, return_to_settings).start()
                
            except Exception as e:
                logger.error(f"Error updating admin message: {e}")
        else:
            bot.send_message(
                message.chat.id,
                f"⚠️ **Word already exists or invalid**\n\n"
                f"'{new_word}' is already in the banned words list or is invalid.",
                parse_mode='Markdown'
            )
    else:
        bot.send_message(
            message.chat.id,
            "❌ **Invalid word**\n\nPlease provide a valid word to ban.",
            parse_mode='Markdown'
        )

def update_deletion_time_setting(call, deletion_time):
    """Update message deletion time setting"""
    if deletion_time == 0:
        # Disable auto-delete
        data_manager.update_admin_setting("auto_delete_enabled", False)
        data_manager.update_admin_setting("message_deletion_time", 300)  # Keep default time but disable
        status_msg = "✅ **Auto-delete disabled**\n\nMessages will no longer be automatically deleted."
    else:
        # Enable auto-delete with specified time
        data_manager.update_admin_setting("auto_delete_enabled", True)
        data_manager.update_admin_setting("message_deletion_time", deletion_time)
        status_msg = f"✅ **Auto-delete updated**\n\nMessages will now be deleted after {deletion_time // 60} minutes."
    
    # Log the admin action
    logger.info(f"Admin {call.from_user.id} updated deletion time to {deletion_time} seconds")
    
    bot.edit_message_text(
        f"{status_msg}\n\nReturning to settings in 3 seconds...",
        call.message.chat.id,
        call.message.message_id,
        parse_mode='Markdown'
    )
    
    # Schedule return to settings
    def return_to_settings():
        try:
            show_message_deletion_settings(call)
        except Exception as e:
            logger.error(f"Error returning to deletion settings: {e}")
    
    threading.Timer(3.0, return_to_settings).start()

def show_banned_words_settings(call):
    """Show banned words management interface"""
    banned_words = data_manager.get_banned_words()
    banned_words_enabled = data_manager.get_admin_setting("banned_words_enabled", True)
    
    settings_text = f"🚫 **Banned Words Management**\n\n"
    settings_text += f"⚙️ **Status:** {'✅ Enabled' if banned_words_enabled else '❌ Disabled'}\n"
    settings_text += f"📝 **Total Banned Words:** {len(banned_words)}\n\n"
    
    if banned_words:
        settings_text += f"📋 **Current Banned Words:**\n"
        for word in banned_words[:10]:  # Show first 10
            settings_text += f"• `{escape_markdown(word)}`\n"
        if len(banned_words) > 10:
            settings_text += f"• ... and {len(banned_words) - 10} more\n"
    else:
        settings_text += f"📋 **No banned words configured**\n"
    
    settings_text += f"\n💡 **How it works:**\n"
    settings_text += f"Messages containing banned words are automatically deleted."
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            f"{'🔴 Disable' if banned_words_enabled else '🟢 Enable'} Banned Words",
            callback_data="toggle_banned_words"
        )
    )
    keyboard.add(InlineKeyboardButton("➕ Add New Word", callback_data="add_banned_word"))
    
    # Add remove word buttons for existing words
    if banned_words:
        for word in banned_words[:5]:  # Show remove buttons for first 5
            keyboard.add(
                InlineKeyboardButton(
                    f"❌ Remove '{word}'",
                    callback_data=f"remove_banned_word_{word}"
                )
            )
    
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main"))
    
    bot.edit_message_text(
        settings_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

def show_link_analytics(call):
    """Show link analytics and statistics"""
    all_stats = data_manager.get_all_stats()
    users_data = all_stats.get("users", {})
    
    # Calculate analytics
    total_links = 0
    platform_stats = {}
    recent_links = []
    
    for user_data in users_data.values():
        user_links = user_data.get("all_links", [])
        total_links += len(user_links)
        
        for link in user_links:
            platform = link.get("type", "Other")
            platform_stats[platform] = platform_stats.get(platform, 0) + 1
            recent_links.append({
                "url": link.get("url", ""),
                "timestamp": link.get("timestamp", ""),
                "type": platform,
                "user": user_data.get("first_name", "Unknown")
            })
    
    # Sort recent links by timestamp
    recent_links.sort(key=lambda x: x["timestamp"], reverse=True)
    
    analytics_text = f"📊 **Link Analytics Dashboard**\n\n"
    analytics_text += f"📈 **Overview:**\n"
    analytics_text += f"• Total Links Tracked: {total_links}\n"
    analytics_text += f"• Active Users: {len([u for u in users_data.values() if u.get('link_count', 0) > 0])}\n"
    analytics_text += f"• Platforms Detected: {len(platform_stats)}\n\n"
    
    # Top platforms
    if platform_stats:
        analytics_text += f"🌐 **Top Platforms:**\n"
        for platform, count in sorted(platform_stats.items(), key=lambda x: x[1], reverse=True)[:8]:
            percentage = (count / total_links * 100) if total_links > 0 else 0
            analytics_text += f"• {escape_markdown(platform)}: {count} links ({percentage:.1f}%)\n"
    
    # Recent activity
    analytics_text += f"\n🕒 **Recent Links** (Last 10):\n"
    if recent_links:
        for link in recent_links[:10]:
            platform = escape_markdown(link["type"])
            user = escape_markdown(link["user"][:15])
            timestamp = link["timestamp"][:10]
            analytics_text += f"• {platform} by {user} \\[{timestamp}\\]\n"
    else:
        analytics_text += f"• No links tracked yet\n"
    
    # User rankings
    user_link_counts = [(u.get("first_name", "Unknown"), u.get("link_count", 0)) 
                       for u in users_data.values() if u.get("link_count", 0) > 0]
    user_link_counts.sort(key=lambda x: x[1], reverse=True)
    
    if user_link_counts:
        analytics_text += f"\n👑 **Top Link Senders:**\n"
        for name, count in user_link_counts[:5]:
            analytics_text += f"• {escape_markdown(name)}: {count} links\n"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔄 Refresh Analytics", callback_data="admin_link_analytics"))
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main"))
    
    bot.edit_message_text(
        analytics_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

def show_bot_settings(call):
    """Show general bot settings"""
    admin_settings = data_manager.get_all_stats().get("admin_settings", {})
    
    max_file_size_mb = admin_settings.get("max_file_size_mb", 200)
    auto_delete_enabled = admin_settings.get("auto_delete_enabled", True)
    deletion_time = admin_settings.get("message_deletion_time", 300)
    banned_words_enabled = admin_settings.get("banned_words_enabled", True)
    channel_join_required = admin_settings.get("channel_join_required", True)
    auto_removal_enabled = admin_settings.get("auto_removal_enabled", True)
    
    settings_text = f"⚙️ **Bot Settings Overview**\n\n"
    settings_text += f"📁 **File Settings:**\n"
    settings_text += f"• Max File Size: {max_file_size_mb} MB\n"
    settings_text += f"• Telegram Limit: {MAX_FILE_SIZE // (1024*1024)} MB\n"
    settings_text += f"• Auto-Removal: {'✅ Enabled' if auto_removal_enabled else '❌ Disabled'}\n\n"
    
    settings_text += f"🗑️ **Message Settings:**\n"
    settings_text += f"• Auto-Delete: {'✅ Enabled' if auto_delete_enabled else '❌ Disabled'}\n"
    settings_text += f"• Deletion Time: {deletion_time // 60} minutes\n\n"
    
    settings_text += f"🚫 **Moderation:**\n"
    settings_text += f"• Banned Words: {'✅ Active' if banned_words_enabled else '❌ Disabled'}\n"
    settings_text += f"• Channel Join: {'✅ Required' if channel_join_required else '❌ Optional'}\n\n"
    
    settings_text += f"📊 **System Status:**\n"
    settings_text += f"• Downloads Directory: {DOWNLOADS_DIR}\n"
    settings_text += f"• Data File: {DATA_FILE}\n"
    
    # Add file count
    try:
        import os
        total_files = len(os.listdir(DOWNLOADS_DIR)) if os.path.exists(DOWNLOADS_DIR) else 0
        settings_text += f"• Files in Downloads: {total_files}\n"
    except:
        settings_text += f"• Files in Downloads: Error reading\n"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔄 Refresh Settings", callback_data="admin_bot_settings"))
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_main"))
    
    bot.edit_message_text(
        settings_text,
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in waiting_for_new_pin and message.text, content_types=['text'])
def handle_new_pin_input(message):
    """Handle new PIN input"""
    user = message.from_user
    if not user or user.id not in waiting_for_new_pin:
        return
    
    new_pin = message.text.strip() if message.text else ""
    original_message_id = waiting_for_new_pin[user.id]
    
    del waiting_for_new_pin[user.id]
    
    # Delete input message immediately for security
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except:
        pass
    
    # Validate PIN
    if len(new_pin) == 6 and new_pin.isdigit():
        # Update admin PIN in environment (for current session)
        global ADMIN_PIN
        ADMIN_PIN = new_pin
        
        # Save to admin settings for persistence
        data_manager.update_admin_setting("admin_pin", new_pin)
        
        bot.edit_message_text(
            f"✅ **Admin PIN Updated Successfully!**\n\n"
            f"🔒 Your new PIN has been set securely.\n"
            f"🔑 Use this PIN for admin access: `{new_pin}`\n\n"
            f"⚠️ **Important:**\n"
            f"• Store this PIN safely\n"
            f"• Takes effect immediately\n"
            f"• Old PIN is no longer valid\n\n"
            f"Returning to settings in 5 seconds...",
            message.chat.id,
            original_message_id,
            parse_mode='Markdown'
        )
        
        def return_to_settings():
            try:
                class MockCall:
                    def __init__(self):
                        self.message = message
                        self.from_user = user
                mock_call = MockCall()
                mock_call.message.message_id = original_message_id
                show_admin_dashboard_callback(mock_call)
            except Exception as e:
                logger.error(f"Error returning to admin panel: {e}")
        
        threading.Timer(5.0, return_to_settings).start()
        logger.info(f"Admin {user.id} successfully changed admin PIN")
    else:
        bot.edit_message_text(
            f"❌ **Invalid PIN Format**\n\n"
            f"PIN must be exactly 6 digits (0-9).\n"
            f"Received: {len(new_pin)} characters\n\n"
            f"Please try again with a valid 6-digit PIN.",
            message.chat.id,
            original_message_id,
            parse_mode='Markdown'
        )

@bot.message_handler(func=lambda message: message.from_user and message.from_user.id in waiting_for_admin_message and message.text, content_types=['text'])
def handle_admin_message_input(message):
    """Handle admin message to user input"""
    user = message.from_user
    if not user or user.id not in waiting_for_admin_message:
        return
    
    admin_message = message.text.strip() if message.text else ""
    chat_info = waiting_for_admin_message[user.id]
    target_user_id = chat_info['target_user_id']
    target_name = chat_info['target_name']
    original_message_id = chat_info['message_id']
    
    del waiting_for_admin_message[user.id]
    
    # Delete input message
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except:
        pass
    
    if admin_message:
        try:
            # Send message to target user
            bot.send_message(
                target_user_id,
                f"📨 **Message from Bot Admin**\n\n"
                f"{admin_message}\n\n"
                f"💬 This message was sent by the bot administrator.\n"
                f"You can continue using the bot normally.",
                parse_mode='Markdown'
            )
            
            # Confirm to admin
            bot.edit_message_text(
                f"✅ **Message Sent Successfully!**\n\n"
                f"👤 **To:** {target_name} (ID: {target_user_id})\n"
                f"📝 **Message:** {admin_message[:100]}{'...' if len(admin_message) > 100 else ''}\n\n"
                f"User has received your message.\n"
                f"Returning to admin panel in 3 seconds...",
                message.chat.id,
                original_message_id,
                parse_mode='Markdown'
            )
            
            def return_to_admin():
                try:
                    class MockCall:
                        def __init__(self):
                            self.message = message
                            self.from_user = user
                    mock_call = MockCall()
                    mock_call.message.message_id = original_message_id
                    show_admin_dashboard_callback(mock_call)
                except Exception as e:
                    logger.error(f"Error returning to admin panel: {e}")
            
            threading.Timer(3.0, return_to_admin).start()
            logger.info(f"Admin {user.id} sent message to user {target_user_id}: {admin_message[:50]}")
            
        except Exception as e:
            bot.edit_message_text(
                f"❌ **Failed to Send Message**\n\n"
                f"Error: {str(e)}\n\n"
                f"User may have blocked the bot or the ID is invalid.",
                message.chat.id,
                original_message_id,
                parse_mode='Markdown'
            )
            logger.error(f"Failed to send admin message to user {target_user_id}: {e}")
    else:
        bot.send_message(message.chat.id, "❌ Message cannot be empty. Please try again.")

def handle_admin_pin_change_request(call):
    """Handle admin PIN change request"""
    user = call.from_user
    if not user:
        return
    
    # Add user to waiting list
    waiting_for_new_pin[user.id] = call.message.message_id
    
    bot.edit_message_text(
        f"🔐 **Change Admin PIN**\n\n"
        f"🔑 **Current PIN:** `{ADMIN_PIN}`\n\n"
        f"📝 **Instructions:**\n"
        f"• Enter a new 6-digit PIN (numbers only)\n"
        f"• PIN must contain exactly 6 digits (0-9)\n"
        f"• This will replace the current PIN immediately\n\n"
        f"⚠️ **Security Notice:**\n"
        f"Your message will be deleted immediately for security.\n\n"
        f"**Please type your new 6-digit PIN now:**",
        call.message.chat.id,
        call.message.message_id,
        parse_mode='Markdown'
    )
    
    logger.info(f"Admin {user.id} initiated PIN change request")

def handle_remove_all_downloads_request(call):
    """Handle remove all downloads request with confirmation"""
    user = call.from_user
    if not user:
        return
    
    # Count current files
    try:
        file_count = len(os.listdir(DOWNLOADS_DIR)) if os.path.exists(DOWNLOADS_DIR) else 0
        
        # Calculate total size
        total_size = 0
        if os.path.exists(DOWNLOADS_DIR):
            for file in os.listdir(DOWNLOADS_DIR):
                file_path = os.path.join(DOWNLOADS_DIR, file)
                if os.path.isfile(file_path):
                    total_size += os.path.getsize(file_path)
    except:
        file_count = 0
        total_size = 0
    
    # Create confirmation keyboard
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("✅ Yes, Remove All", callback_data="confirm_remove_all_downloads"),
        InlineKeyboardButton("❌ Cancel", callback_data="admin_file_management")
    )
    
    bot.edit_message_text(
        f"🗑️ **Remove All Downloads**\n\n"
        f"⚠️ **WARNING:** This action cannot be undone!\n\n"
        f"📁 **Current Status:**\n"
        f"• Files to remove: {file_count}\n"
        f"• Total size: {format_file_size(total_size)}\n"
        f"• Directory: `{DOWNLOADS_DIR}`\n\n"
        f"🚨 **This will:**\n"
        f"• Delete ALL downloaded video files\n"
        f"• Clear ALL scheduled auto-removals\n"
        f"• Free up disk space immediately\n"
        f"• Cannot be reversed\n\n"
        f"**Are you sure you want to proceed?**",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

def main():
    """Main function to start the bot with proper error handling"""
    try:
        logger.info("Starting Telegram Video Download Bot...")
        logger.info(f"Bot token configured: {'Yes' if BOT_TOKEN else 'No'}")
        logger.info(f"Admin PIN: {ADMIN_PIN}")
        logger.info(f"Downloads directory: {DOWNLOADS_DIR}")
        logger.info(f"Max file size: {format_file_size(MAX_FILE_SIZE)}")
        logger.info(f"Auto-removal enabled: {data_manager.get_admin_setting('auto_removal_enabled', True)}")
        
        # Create downloads directory
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        
        # Test bot connection and clear any existing webhooks
        try:
            me = bot.get_me()
            logger.info(f"Bot connected successfully: @{me.username} ({me.first_name})")
            
            # Clear any existing webhooks that might cause conflicts
            bot.remove_webhook()
            time.sleep(1)  # Brief pause
            
        except Exception as connection_error:
            logger.error(f"Failed to connect to bot: {connection_error}")
            raise
        
        # Start polling with improved error handling
        logger.info("Bot is running... Press Ctrl+C to stop")
        
        # Use polling with proper exception handling for 409 conflicts
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                bot.infinity_polling(
                    none_stop=True, 
                    interval=2,  # Increased interval to reduce conflicts
                    timeout=30,  # Increased timeout
                    long_polling_timeout=20,
                    logger_level=logging.WARNING  # Reduce log noise
                )
                break  # If successful, break the retry loop
                
            except Exception as polling_error:
                error_str = str(polling_error).lower()
                
                if "409" in error_str or "conflict" in error_str:
                    retry_count += 1
                    wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30 seconds
                    logger.warning(f"Bot instance conflict detected (attempt {retry_count}/{max_retries}). Retrying in {wait_time} seconds...")
                    
                    if retry_count >= max_retries:
                        logger.error("Max retries exceeded for bot conflicts. Exiting.")
                        break
                    
                    time.sleep(wait_time)
                    
                    # Try to clear any lingering connections
                    try:
                        bot.remove_webhook()
                        bot.close()
                    except:
                        pass
                        
                    continue
                    
                else:
                    # Different error, re-raise it
                    logger.error(f"Non-conflict polling error: {polling_error}")
                    raise
                    
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (Ctrl+C)")
        try:
            bot.stop_polling()
            bot.close()
        except:
            pass
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Critical error starting bot: {e}")
        try:
            bot.stop_polling()
            bot.close()
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()