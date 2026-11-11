"""
Super Bot — Telegram media downloader (Telethon + yt-dlp + Instaloader)

REQUIRED environment variables:
    API_ID           Telegram API id        (https://my.telegram.org)
    API_HASH         Telegram API hash
    TELEGRAM_TOKEN   Bot token from @BotFather
    KEY              Password users must send to unlock the bot

OPTIONAL environment variables:
    ADMIN_IDS                 Comma-separated Telegram user IDs that are always
                               authorized (no password needed) and can see the
                               admin panel. e.g. "111111,222222"
    MAX_CONCURRENT_DOWNLOADS  How many downloads may run at once, across all
                               users (default 3).
    MAX_QUEUE_PER_USER        Max queued items per user (default 10).

STATE PERSISTENCE:
    Authorized users / modes / stats are saved to ./bot_state.json. If this is
    running inside GitHub Actions (GITHUB_REPOSITORY + a token are present),
    the bot also periodically commits that file back to the repo, so a login
    survives the workflow's automatic restart. Outside Actions it's just a
    local file, which still protects you if the bot process itself restarts.
"""

import os
import json
import signal
import logging
import asyncio
import time
import math
import glob
import io
import subprocess
import random
import re
import shutil
import zipfile

import yt_dlp
import requests
from telethon import TelegramClient, events, Button, errors
from telethon.tl.types import DocumentAttributeVideo, DocumentAttributeAudio

try:
    import instaloader
except ImportError:
    instaloader = None
    logging.error("Instaloader not found. Please add 'instaloader' to requirements.txt")

# --- LOGGING ---
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("instaloader").setLevel(logging.WARNING)

try:
    import cryptg  # noqa: F401
    logging.info("✅ cryptg is installed — Telethon will use fast native encryption.")
except ImportError:
    logging.warning(
        "⚠️ cryptg is NOT installed — uploads/downloads will use slow pure-Python "
        "encryption. Check that `pip install -r requirements.txt` actually built it "
        "(it needs a C compiler on the runner; ubuntu-latest has one by default)."
    )


# ============================================================================
# CONFIGURATION
# ============================================================================

def _require_env(name):
    val = os.getenv(name)
    if not val:
        raise SystemExit(
            f"FATAL: required environment variable '{name}' is not set. "
            f"Check your workflow's `env:` block / repo secrets."
        )
    return val


API_ID = int(_require_env('API_ID'))
API_HASH = _require_env('API_HASH')
BOT_TOKEN = _require_env('TELEGRAM_TOKEN')
SECRET_KEY = _require_env('KEY')

ADMIN_IDS = set()
for _part in os.getenv('ADMIN_IDS', '').split(','):
    _part = _part.strip()
    if _part.isdigit():
        ADMIN_IDS.add(int(_part))

MAX_CONCURRENT_DOWNLOADS = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '3'))
MAX_QUEUE_PER_USER = int(os.getenv('MAX_QUEUE_PER_USER', '10'))
TELEGRAM_FILE_LIMIT = 2 * 1024 ** 3  # 2GB

LOGIN_MAX_ATTEMPTS = 5
LOGIN_LOCKOUT_SECONDS = 15 * 60

STATE_FILE = "bot_state.json"
AUTOSAVE_INTERVAL_SECONDS = 5 * 60

DESKTOP_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
MOBILE_UA = ('Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) '
              'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1')

VIDEO_EXTS = ('.mp4', '.mkv', '.avi', '.mov', '.webm', '.ts', '.flv')
PHOTO_EXTS = ('.png', '.jpg', '.jpeg', '.webp')

MAX_ZIP_FILES = 500
MAX_ZIP_UNCOMPRESSED_BYTES = 4 * 1024 ** 3  # 4GB safety cap

START_TIME = time.time()

# --- INITIALIZE CLIENT (started inside main()) ---
client = TelegramClient('bot_session', API_ID, API_HASH)

# --- STATE (populated from disk at startup, see bottom of file) ---
AUTHORIZED_USERS = set()
USER_DATA = {}
TASK_CANCEL = {}
STATS = {}            # {str(user_id): {"downloads": int, "bytes": int}}
LOGIN_ATTEMPTS = {}    # {user_id: {"count": int, "locked_until": float}}

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
STATE_LOCK = asyncio.Lock()


# ============================================================================
# PERSISTENCE
# ============================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Could not read {STATE_FILE}: {e}")
    return {"authorized_users": [], "user_modes": {}, "stats": {}}


async def save_state_async():
    async with STATE_LOCK:
        data = {
            "authorized_users": list(AUTHORIZED_USERS),
            "user_modes": {str(uid): d["mode"] for uid, d in USER_DATA.items() if d.get("mode")},
            "stats": STATS,
        }
        try:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            logging.error(f"Failed to write {STATE_FILE}: {e}")


async def git_commit_and_push():
    """Best-effort: commit bot_state.json back to the repo so it survives the
    GitHub Actions restart cycle. Silently does nothing if not running there,
    and never raises — persistence is a convenience, not a requirement."""
    repo = os.getenv("GITHUB_REPOSITORY")
    if not repo:
        return  # not running inside GitHub Actions

    async def run(*cmd):
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            await proc.communicate()
            return proc.returncode
        except Exception as e:
            logging.debug(f"git command failed {cmd}: {e}")
            return 1

    try:
        token = os.getenv("GITHUB_TOKEN")
        if token:
            await run("git", "remote", "set-url", "origin",
                       f"https://x-access-token:{token}@github.com/{repo}.git")
        await run("git", "config", "user.email", "actions@github.com")
        await run("git", "config", "user.name", "github-actions[bot]")
        await run("git", "add", STATE_FILE)
        rc = await run("git", "commit", "-m", "chore: persist bot state [skip ci]")
        if rc == 0:
            ref = os.getenv("GITHUB_REF_NAME", "main")
            push_rc = await run("git", "push", "origin", f"HEAD:{ref}")
            if push_rc == 0:
                logging.info("Bot state pushed to repo.")
            else:
                logging.warning("Could not push bot state (will retry later).")
    except Exception as e:
        logging.warning(f"Git state persistence skipped: {e}")


async def persist_state(push=False):
    await save_state_async()
    if push:
        await git_commit_and_push()


async def autosave_loop():
    while True:
        await asyncio.sleep(AUTOSAVE_INTERVAL_SECONDS)
        await persist_state(push=True)


# ============================================================================
# HELPERS
# ============================================================================

def get_user_data(user_id):
    return USER_DATA.setdefault(user_id, {})


def is_admin(user_id):
    return user_id in ADMIN_IDS


def is_auth(user_id):
    return user_id in AUTHORIZED_USERS or is_admin(user_id)


def format_size(size_bytes):
    if not size_bytes:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    i = min(i, len(size_name) - 1)
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"


def get_progress_bar(current, total, start_time):
    elapsed = time.time() - start_time
    speed = current / elapsed if elapsed > 0 else 0
    speed_str = f"{format_size(speed)}/s" if speed > 0 else "0B/s"

    if not total:
        return f"📊 {format_size(current)} downloaded\n🚀 {speed_str}"

    percent = min(int(current / total * 100), 100)
    bar_len = 10
    filled = int(percent / 100 * bar_len)
    bar = '■' * filled + '□' * (bar_len - filled)

    eta_str = "…"
    if speed > 0 and total > current:
        eta_sec = int((total - current) / speed)
        m, s = divmod(eta_sec, 60)
        eta_str = f"{m}m {s}s" if m else f"{s}s"

    return f"[{bar}] {percent}%\n📊 {format_size(current)} / {format_size(total)}\n🚀 {speed_str} • ⏱ ETA {eta_str}"


async def safe_edit(message, text, buttons=None):
    try:
        await message.edit(text, buttons=buttons)
        return message
    except (errors.MessageIdInvalidError, errors.MessageNotModifiedError):
        try:
            return await message.respond(text, buttons=buttons)
        except Exception as e:
            logging.debug(f"safe_edit fallback respond failed: {e}")
            return message
    except Exception as e:
        logging.debug(f"safe_edit error: {e}")
        return message


def update_stats(user_id, count, total_bytes):
    key = str(user_id)
    s = STATS.setdefault(key, {"downloads": 0, "bytes": 0})
    s["downloads"] += count
    s["bytes"] += total_bytes
    asyncio.create_task(save_state_async())


# --- Login lockout (brute-force protection) ---

def check_login_lock(user_id):
    rec = LOGIN_ATTEMPTS.get(user_id)
    if rec and rec.get('locked_until', 0) > time.time():
        return int(rec['locked_until'] - time.time())
    return 0


def register_login_failure(user_id):
    rec = LOGIN_ATTEMPTS.setdefault(user_id, {'count': 0, 'locked_until': 0})
    rec['count'] += 1
    if rec['count'] >= LOGIN_MAX_ATTEMPTS:
        rec['locked_until'] = time.time() + LOGIN_LOCKOUT_SECONDS
        rec['count'] = 0


def clear_login_failures(user_id):
    LOGIN_ATTEMPTS.pop(user_id, None)


# ============================================================================
# ASYNC MEDIA HELPERS (non-blocking ffprobe/ffmpeg — see note in chat reply)
# ============================================================================

async def probe_media(filepath):
    """Returns (duration, width, height) via a single non-blocking ffprobe call.
    Returns zeros on any failure (never raises)."""
    cmd = ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
           '-show_entries', 'format=duration:stream=width,height',
           '-of', 'json', filepath]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode() or '{}')
        duration = int(float(data.get('format', {}).get('duration', 0) or 0))
        streams = data.get('streams', [])
        width = int(streams[0].get('width', 0)) if streams else 0
        height = int(streams[0].get('height', 0)) if streams else 0
        return duration, width, height
    except Exception as e:
        logging.debug(f"probe_media failed for {filepath}: {e}")
        return 0, 0, 0


async def make_thumbnail(filepath):
    thumb_path = f"{filepath}.jpg"
    cmd = ['ffmpeg', '-y', '-i', filepath, '-ss', '00:00:01', '-vframes', '1', thumb_path]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        await proc.communicate()
        return thumb_path if os.path.exists(thumb_path) else None
    except Exception as e:
        logging.debug(f"make_thumbnail failed for {filepath}: {e}")
        return None


async def build_video_attributes(filepath):
    duration, width, height = await probe_media(filepath)
    if not (width and height):
        return []
    return [DocumentAttributeVideo(duration=duration, w=width, h=height, supports_streaming=True)]


def _is_faststart(filepath):
    """Walks top-level MP4/MOV box headers to check whether 'moov' already
    comes before 'mdat' (i.e. already faststart). Pure Python, no subprocess
    — just a handful of small reads from the front of the file. Returns
    False (meaning: remux it) if the layout can't be determined, to stay safe."""
    try:
        with open(filepath, 'rb') as f:
            file_size = os.fstat(f.fileno()).st_size
            pos = 0
            while pos < file_size - 8:
                f.seek(pos)
                header = f.read(8)
                if len(header) < 8:
                    break
                size = int.from_bytes(header[0:4], 'big')
                box_type = header[4:8].decode('ascii', errors='ignore')
                if box_type == 'moov':
                    return True
                if box_type == 'mdat':
                    return False
                if size == 1:  # 64-bit "largesize" follows the header
                    size_bytes = f.read(8)
                    if len(size_bytes) < 8:
                        break
                    size = int.from_bytes(size_bytes, 'big')
                if size < 8:  # malformed / "extends to EOF" — bail out safely
                    break
                pos += size
    except Exception as e:
        logging.debug(f"_is_faststart check failed for {filepath}: {e}")
    return False


async def ensure_faststart(filepath, status_msg):
    if not os.path.exists(filepath) or not filepath.lower().endswith(('.mp4', '.mov')):
        return filepath

    loop = asyncio.get_event_loop()
    already_fast = await loop.run_in_executor(None, _is_faststart, filepath)
    if already_fast:
        logging.info(f"{filepath} is already faststart — skipping remux")
        return filepath

    await safe_edit(status_msg, "🚀 **Optimizing for streaming...**")
    base, ext = os.path.splitext(filepath)
    output_path = f"{base}_stream{ext}"
    cmd = ['ffmpeg', '-y', '-i', filepath, '-c', 'copy', '-movflags', '+faststart', output_path]
    process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await process.communicate()

    if os.path.exists(output_path):
        os.remove(filepath)
        return output_path
    return filepath


async def transcode_video(filepath, target_height, status_msg):
    if not os.path.exists(filepath) or not target_height:
        return filepath

    _, _, current_height = await probe_media(filepath)
    if not current_height or current_height <= target_height:
        return filepath

    await safe_edit(status_msg, f"⚙️ **Processing...**\nConverting {current_height}p ➡️ {target_height}p")
    base, ext = os.path.splitext(filepath)
    output_path = f"{base}_{target_height}p{ext}"
    cmd = ['ffmpeg', '-y', '-i', filepath, '-vf', f"scale=-2:{target_height}",
           '-c:v', 'libx264', '-preset', 'superfast', '-crf', '28',
           '-c:a', 'copy', '-movflags', '+faststart', output_path]
    process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await process.communicate()

    if os.path.exists(output_path):
        os.remove(filepath)
        return output_path
    return filepath


# ============================================================================
# INSTAGRAM (Instaloader) — videos, photos and carousels
# ============================================================================

def download_instagram_content(url, work_dir):
    """Downloads all media (video/photo, including carousels) from an
    Instagram post. Returns a list of file paths. Runs in a thread executor
    since Instaloader is synchronous."""
    L = instaloader.Instaloader(
        download_pictures=True,
        download_videos=True,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
    )

    shortcode_match = re.search(r'(?:/p/|/reel/|/tv/)([\w-]+)', url)
    if not shortcode_match:
        raise ValueError("Unsupported Instagram URL (only post/reel/tv links are supported)")
    shortcode = shortcode_match.group(1)

    temp_target = os.path.join(work_dir, f"ig_{shortcode}_{random.randint(1000, 9999)}")
    files = []
    try:
        post = instaloader.Post.from_shortcode(L.context, shortcode)
        L.download_post(post, target=temp_target)

        videos = sorted(glob.glob(f"{temp_target}/*.mp4"))
        photos = sorted(glob.glob(f"{temp_target}/*.jpg")) + sorted(glob.glob(f"{temp_target}/*.webp"))
        for i, src in enumerate(videos + photos):
            ext = os.path.splitext(src)[1]
            dest = os.path.join(work_dir, f"Instagram_{shortcode}_{i}{ext}")
            shutil.move(src, dest)
            files.append(dest)
    finally:
        if os.path.exists(temp_target):
            shutil.rmtree(temp_target, ignore_errors=True)
    return files


# ============================================================================
# ZIP HANDLING — Zip-Slip safe extraction
# ============================================================================

def safe_extract_zip(zip_path, extract_path):
    """Extracts a zip safely: blocks path traversal (Zip Slip) and caps total
    size / file count against zip-bomb style abuse. Raises ValueError or
    zipfile.BadZipFile on anything suspicious."""
    os.makedirs(extract_path, exist_ok=True)
    extract_root = os.path.realpath(extract_path)

    with zipfile.ZipFile(zip_path, 'r') as zf:
        infos = zf.infolist()
        if len(infos) > MAX_ZIP_FILES:
            raise ValueError(f"Zip contains too many files ({len(infos)} > {MAX_ZIP_FILES})")
        total_size = sum(i.file_size for i in infos)
        if total_size > MAX_ZIP_UNCOMPRESSED_BYTES:
            raise ValueError(f"Zip is too large when extracted ({format_size(total_size)})")
        for info in infos:
            dest = os.path.realpath(os.path.join(extract_path, info.filename))
            if not (dest == extract_root or dest.startswith(extract_root + os.sep)):
                raise ValueError(f"Unsafe path in zip entry: {info.filename}")
        zf.extractall(extract_path)


async def send_media_batch(event, files, status_msg=None):
    """Sends a list of media files one by one with progress updates.
    Returns (sent_count, failed_count)."""
    total = len(files)
    sent = 0
    failed = 0
    for idx, file_path in enumerate(files, 1):
        is_video = file_path.lower().endswith(VIDEO_EXTS)
        file_name = os.path.basename(file_path)
        if status_msg:
            await safe_edit(status_msg, f"📤 **Sending {idx}/{total}**\n`{file_name}`")

        thumb_path = None
        try:
            if is_video:
                attributes = await build_video_attributes(file_path)
                thumb_path = await make_thumbnail(file_path)
                await client.send_file(
                    event.chat_id, file_path, attributes=attributes,
                    supports_streaming=True,
                    thumb=thumb_path if thumb_path else None,
                )
            else:
                await client.send_file(event.chat_id, file_path)
            sent += 1
        except Exception as e:
            logging.error(f"Failed to send {file_path}: {e}")
            failed += 1
        finally:
            if thumb_path and os.path.exists(thumb_path):
                os.remove(thumb_path)
    return sent, failed


async def process_zip_file(event):
    status_msg = await event.respond("📥 **Downloading ZIP file...**")
    user_id = event.sender_id
    timestamp = int(time.time())
    zip_path = f"downloads/temp_{user_id}_{timestamp}.zip"
    extract_path = f"downloads/extracted_{user_id}_{timestamp}"
    os.makedirs("downloads", exist_ok=True)

    try:
        await event.download_media(file=zip_path)
        await safe_edit(status_msg, "📦 **Extracting ZIP file...**")

        try:
            safe_extract_zip(zip_path, extract_path)
        except zipfile.BadZipFile:
            await safe_edit(status_msg, "❌ **Error: Not a valid ZIP file.**")
            return
        except ValueError as e:
            await safe_edit(status_msg, f"❌ **Rejected:** {e}")
            return

        all_files = [os.path.join(r, f) for r, _, fs in os.walk(extract_path) for f in fs]
        photos = [f for f in all_files if f.lower().endswith(PHOTO_EXTS)]
        videos = [f for f in all_files if f.lower().endswith(VIDEO_EXTS)]
        media_files = videos + photos

        if not media_files:
            await safe_edit(status_msg, "⚠️ **No photos or videos found in the ZIP.**")
            return

        await safe_edit(
            status_msg,
            f"📊 **Extracted {len(all_files)} files**\n"
            f"📸 Photos: {len(photos)}  🎥 Videos: {len(videos)}\n\n"
            f"🚀 Sending {len(media_files)} media files..."
        )
        sent, failed = await send_media_batch(event, media_files, status_msg)
        summary = f"✅ **Sent {sent}/{len(media_files)} files**"
        if failed:
            summary += f"\n⚠️ {failed} file(s) failed to send"
        await safe_edit(status_msg, summary)

    except Exception as e:
        logging.error(f"ZIP processing error: {e}")
        await safe_edit(status_msg, f"❌ **Error processing ZIP:** {str(e)[:150]}")
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path, ignore_errors=True)
        await send_main_menu(event)


# ============================================================================
# MENUS / TEXT
# ============================================================================

HELP_TEXT = (
    "ℹ️ **How to use this bot**\n\n"
    "**Download Video** — send one link to pick a quality, or paste several "
    "links (one per line) to queue them all at best quality in one go.\n"
    "🎵 Pick *Audio Only* on the quality menu to get an MP3 instead of video.\n\n"
    "**Screenshot** — send a URL to capture a snapshot of the page.\n\n"
    "**Extract and Send 📦** — send a ZIP and the bot unpacks and forwards every photo/video inside.\n\n"
    "**Fast Mode ⚡** uses aria2c for raw speed (best for direct MP4s/large files).\n"
    "**Normal Mode 🐢** uses yt-dlp's native downloader (best for Instagram/YouTube/HLS).\n\n"
    "**Show Queue 📋** lists what's waiting. Use ❌ **Cancel** on an active task to stop it "
    "without touching the rest of your queue.\n\n"
    "Instagram links are always handled automatically, regardless of mode."
)


def build_stats_text(user_id):
    s = STATS.get(str(user_id), {"downloads": 0, "bytes": 0})
    return (
        f"📊 **Your Stats**\n\n"
        f"✅ Downloads: **{s.get('downloads', 0)}**\n"
        f"💾 Total size: **{format_size(s.get('bytes', 0))}**"
    )


def build_admin_text():
    uptime = int(time.time() - START_TIME)
    h, rem = divmod(uptime, 3600)
    m, _ = divmod(rem, 60)
    total_dl = sum(s.get('downloads', 0) for s in STATS.values())
    total_bytes = sum(s.get('bytes', 0) for s in STATS.values())
    return (
        f"🛠 **Admin Panel**\n\n"
        f"👥 Authorized users: **{len(AUTHORIZED_USERS)}**\n"
        f"✅ Total downloads: **{total_dl}**\n"
        f"💾 Total data moved: **{format_size(total_bytes)}**\n"
        f"⏱ Uptime: **{h}h {m}m**\n"
        f"🔀 Concurrent download slots: **{MAX_CONCURRENT_DOWNLOADS}**"
    )


async def send_main_menu(event):
    user_id = event.sender_id
    mode = get_user_data(user_id).get('mode', 'normal')
    fast_lbl = "✅ Fast Mode ⚡" if mode == 'fast' else "Fast Mode ⚡"
    norm_lbl = "✅ Normal Mode 🐢" if mode == 'normal' else "Normal Mode 🐢"

    buttons = [
        [Button.text("Download Video", resize=True), Button.text("Screenshot")],
        [Button.text("Extract and Send 📦"), Button.text("Show Queue 📋")],
        [Button.text(fast_lbl), Button.text(norm_lbl)],
        [Button.text("📊 My Stats"), Button.text("ℹ️ Help")],
    ]
    if is_admin(user_id):
        buttons.append([Button.text("🛠 Admin Panel")])

    await event.respond(
        f"🚀 **Super Bot Ready**\nLimit: **2GB per file**\nMode: **{mode.title()}**",
        buttons=buttons
    )


BUTTON_TEXTS = {
    'Login', 'Download Video', 'Screenshot', 'Extract and Send 📦',
    '❌ Cancel Task', 'Show Queue 📋', '📊 My Stats', 'ℹ️ Help', '🛠 Admin Panel',
}


# ============================================================================
# HANDLERS — buttons & commands
# ============================================================================

@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    if is_auth(event.sender_id):
        await send_main_menu(event)
    else:
        await event.respond("🔒 **Locked.**", buttons=[[Button.text("Login", resize=True)]])


@client.on(events.NewMessage(pattern='Login'))
async def login_btn(event):
    if is_auth(event.sender_id):
        return await send_main_menu(event)
    locked = check_login_lock(event.sender_id)
    if locked:
        return await event.respond(f"🔒 **Too many attempts.** Try again in {locked // 60 + 1} min.")
    get_user_data(event.sender_id)['state'] = 'waiting_password'
    await event.respond("🔑 **Enter Password:**", buttons=Button.clear())


@client.on(events.NewMessage(pattern='Fast Mode ⚡'))
async def set_fast(event):
    if not is_auth(event.sender_id):
        return
    ud = get_user_data(event.sender_id)
    if ud.get('mode') == 'fast':
        return await event.respond("⚠️ **Fast Mode is already active.**")
    ud['mode'] = 'fast'
    asyncio.create_task(save_state_async())
    await event.respond("⚡ **Fast Mode (Aria2) Active**\nBest for: Direct MP4s, Large Files.")
    await send_main_menu(event)


@client.on(events.NewMessage(pattern='Normal Mode 🐢'))
async def set_normal(event):
    if not is_auth(event.sender_id):
        return
    ud = get_user_data(event.sender_id)
    if ud.get('mode') == 'normal':
        return await event.respond("⚠️ **Normal Mode is already active.**")
    ud['mode'] = 'normal'
    asyncio.create_task(save_state_async())
    await event.respond("🐢 **Normal Mode (Native) Active**\nBest for: Instagram, YouTube, HLS.")
    await send_main_menu(event)


@client.on(events.NewMessage(pattern='Download Video'))
async def dl_btn(event):
    if not is_auth(event.sender_id):
        return
    get_user_data(event.sender_id)['state'] = 'waiting_link'
    await event.respond(
        "🚀 **Send a link.**\nOr paste several links (one per line) to queue them all at best quality.",
        buttons=Button.clear()
    )


@client.on(events.NewMessage(pattern='Screenshot'))
async def ss_btn(event):
    if not is_auth(event.sender_id):
        return
    get_user_data(event.sender_id)['state'] = 'waiting_ss'
    await event.respond("📸 **Send Link for Screenshot:**", buttons=Button.clear())


@client.on(events.NewMessage(pattern='Extract and Send 📦'))
async def extract_btn(event):
    if not is_auth(event.sender_id):
        return
    get_user_data(event.sender_id)['state'] = 'waiting_zip'
    await event.respond("📦 **Send the ZIP file you want to extract:**", buttons=Button.clear())


@client.on(events.NewMessage(pattern='Show Queue 📋'))
async def show_queue_handler(event):
    if not is_auth(event.sender_id):
        return
    queue = get_user_data(event.sender_id).get('queue', [])
    if not queue:
        return await event.respond("📭 **Queue is Empty.**")
    msg = "📋 **Current Queue:**\n\n"
    for i, item in enumerate(queue):
        msg += f"**{i+1}.** {item['title'][:40]}...\n   ├ Quality: {item['quality']}\n   └ Size: {item['size']}\n\n"
    await event.respond(msg)


@client.on(events.NewMessage(pattern='📊 My Stats'))
async def stats_btn(event):
    if not is_auth(event.sender_id):
        return
    await event.respond(build_stats_text(event.sender_id))


@client.on(events.NewMessage(pattern='ℹ️ Help'))
async def help_btn(event):
    if not is_auth(event.sender_id):
        return
    await event.respond(HELP_TEXT)


@client.on(events.NewMessage(pattern='🛠 Admin Panel'))
async def admin_btn(event):
    if not is_admin(event.sender_id):
        return
    await event.respond(build_admin_text())


@client.on(events.NewMessage(pattern='❌ Cancel Task'))
async def cancel_task_msg(event):
    TASK_CANCEL[event.sender_id] = True
    await event.respond("🛑 **Stopping Current Task...**\n(Queue continues)")


# --- TEXT LISTENER ---
@client.on(events.NewMessage)
async def message_handler(event):
    text = event.text or ""
    user_id = event.sender_id

    if text.startswith('/') or text in BUTTON_TEXTS or "Mode" in text:
        return

    ud = get_user_data(user_id)
    state = ud.get('state')

    if state == 'waiting_password':
        try:
            await event.delete()
        except Exception:
            pass

        locked = check_login_lock(user_id)
        if locked:
            await event.respond(f"🔒 **Too many attempts.** Try again in {locked // 60 + 1} min.")
            return

        if text.strip() == SECRET_KEY:
            clear_login_failures(user_id)
            AUTHORIZED_USERS.add(user_id)
            ud['state'] = None
            ud.setdefault('queue', [])
            ud['is_busy'] = False
            asyncio.create_task(save_state_async())
            await event.respond("✅ **Access Granted.**")
            await send_main_menu(event)
        else:
            register_login_failure(user_id)
            await event.respond("❌ **Wrong Password.**")
        return

    if state == 'waiting_ss':
        await process_screenshot(event, text)
        ud['state'] = None
        return

    if state == 'waiting_zip':
        if event.document:
            ud['state'] = None
            await process_zip_file(event)
        else:
            await event.respond("⚠️ **Please send a valid file/ZIP.**")
        return

    is_link = text.strip().startswith("http")
    if (state == 'waiting_link' and is_link) or (is_auth(user_id) and is_link):
        ud['state'] = None
        urls = [l.strip() for l in text.splitlines() if l.strip().lower().startswith('http')]
        if not urls:
            urls = [text.strip()]
        if len(urls) > 1:
            await process_batch_links(event, urls)
        else:
            await process_link_quality(event, urls[0])
        return


# ============================================================================
# CORE FUNCTIONS
# ============================================================================

async def process_screenshot(event, url):
    if not url.startswith("http"):
        url = "https://" + url
    msg = await event.respond("📸 **Capturing...**")
    try:
        api = f"https://s0.wp.com/mshots/v1/{requests.utils.quote(url)}?w=1920&h=1080"
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(None, lambda: requests.get(api, timeout=30))
        f = io.BytesIO(r.content)
        f.name = "screenshot.jpg"
        await client.send_file(event.chat_id, f, caption=f"📸 {url}")
        await msg.delete()
    except Exception as e:
        await safe_edit(msg, f"❌ Error: {str(e)[:150]}")
    await send_main_menu(event)


async def process_batch_links(event, urls):
    user_id = event.sender_id
    ud = get_user_data(user_id)
    queue = ud.setdefault('queue', [])

    room = MAX_QUEUE_PER_USER - len(queue)
    if room <= 0:
        await event.respond(f"⚠️ **Queue full** (max {MAX_QUEUE_PER_USER}). Wait for current tasks to finish.")
        return

    to_add = urls[:room]
    skipped = len(urls) - len(to_add)
    for url in to_add:
        queue.append({'url': url, 'quality': 'best', 'title': url[:60], 'size': 'Unknown'})

    msg = f"✅ **Queued {len(to_add)} link(s)** at best quality."
    if skipped:
        msg += f"\n⚠️ {skipped} skipped (queue limit is {MAX_QUEUE_PER_USER})."
    await event.respond(msg)
    asyncio.create_task(queue_worker(event, user_id))


async def process_link_quality(event, url):
    # Instagram is handled by a dedicated path that doesn't need quality probing
    if "instagram.com" in url:
        await queue_single(event, url, "best", "Instagram Post")
        return

    msg = await event.respond("🔍 **Analyzing Link...**")
    ud = get_user_data(event.sender_id)
    ud['current_url'] = url

    def get_info(ua_type="desktop"):
        ua = DESKTOP_UA if ua_type == "desktop" else MOBILE_UA
        opts = {
            'quiet': True, 'no_warnings': True,
            'user_agent': ua,
            'referer': 'https://www.google.com/',
            'nocheckcertificate': True,
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = None
        try:
            info = await asyncio.get_event_loop().run_in_executor(None, lambda: get_info("desktop"))
        except Exception:
            pass
        if not info or 'formats' not in info:
            info = await asyncio.get_event_loop().run_in_executor(None, lambda: get_info("mobile"))

        video_title = info.get('title', 'Unknown Video') if info else 'Unknown Video'
        ud['temp_title'] = video_title

        size_map = {}
        if info and 'formats' in info:
            for f in info['formats']:
                h = f.get('height')
                s = f.get('filesize') or f.get('filesize_approx')
                if h and s and s > size_map.get(h, 0):
                    size_map[h] = s

        ud['size_map'] = size_map
        ud['best_size'] = (info.get('filesize') or info.get('filesize_approx') or 0) if info else 0

        formats = []
        if info and 'formats' in info:
            seen = set()
            for f in info['formats']:
                h = f.get('height')
                if h and f.get('vcodec') != 'none' and h not in seen:
                    seen.add(h)
                    formats.append(h)
            formats.sort(reverse=True)

        standard_qualities = [1080, 720, 480, 360]
        display_qualities = sorted(set(standard_qualities) | set(formats), reverse=True)
        display_qualities = [q for q in display_qualities if 360 <= q <= 2160]
        if not display_qualities:
            display_qualities = standard_qualities

        buttons = [
            [Button.inline("🌟 Best Quality", data="q_best")],
            [Button.inline("🎵 Audio Only (MP3)", data="q_audio")],
        ]
        row = []
        for res in display_qualities[:4]:
            row.append(Button.inline(f"{res}p", data=f"q_{res}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([Button.inline("❌ Cancel", data="cancel")])

        await msg.delete()
        await event.respond(f"🎞️ **Found:** `{video_title}`\n**Select Quality:**", buttons=buttons)

    except Exception as e:
        logging.warning(f"Could not probe {url}: {e}")
        buttons = [
            [Button.inline("🌟 Default (Force)", data="q_best")],
            [Button.inline("🎵 Audio Only (MP3)", data="q_audio")],
            [Button.inline("❌ Cancel", data="cancel")],
        ]
        await msg.delete()
        await event.respond("⚠️ **Could not probe quality.** Try Force Download:", buttons=buttons)


async def queue_single(event, url, quality, title, size_str="Unknown"):
    user_id = event.sender_id
    ud = get_user_data(user_id)
    queue = ud.setdefault('queue', [])
    if len(queue) >= MAX_QUEUE_PER_USER:
        await event.respond(f"⚠️ **Queue full** (max {MAX_QUEUE_PER_USER}). Wait for current tasks to finish.")
        return
    queue.append({'url': url, 'quality': quality, 'title': title, 'size': size_str})
    await event.respond(f"✅ **Added to Queue** (Position: {len(queue)})\nTitle: `{title}`")
    asyncio.create_task(queue_worker(event, user_id))


@client.on(events.CallbackQuery)
async def callback_handler(event):
    data = event.data.decode('utf-8')
    user_id = event.sender_id

    if data == 'cancel':
        await event.delete()
        return

    if data == 'cancel_task':
        TASK_CANCEL[user_id] = True
        await event.answer("🛑 Stopping...")
        return

    if data == 'show_q_active':
        queue = get_user_data(user_id).get('queue', [])
        if not queue:
            await event.answer("Empty Queue", alert=True)
        else:
            msg = "📋 Current Queue:\n"
            for i, item in enumerate(queue):
                msg += f"{i+1}. {item['title'][:20]}...\n"
            await event.answer(msg, alert=True)
        return

    if data.startswith('q_'):
        ud = get_user_data(user_id)
        url = ud.get('current_url')
        if not url:
            return await event.edit("❌ Link expired.")

        quality = data.split('_', 1)[1]
        title = ud.get('temp_title', 'Unknown Video')

        size_str = "Unknown"
        if quality == 'best':
            best_size = ud.get('best_size', 0)
            if best_size:
                size_str = format_size(best_size)
        elif quality != 'audio':
            size_map = ud.get('size_map', {})
            target_size = size_map.get(int(quality), 0) or ud.get('best_size', 0)
            if target_size:
                size_str = format_size(target_size)
                if target_size > TELEGRAM_FILE_LIMIT:
                    size_str += " ⚠️ over 2GB"

        await event.delete()
        await queue_single(event, url, quality, title, size_str)


# ============================================================================
# DOWNLOAD ENGINE
# ============================================================================

async def run_download(event, url, quality):
    user_id = event.sender_id
    TASK_CANCEL[user_id] = False

    task_buttons = [[Button.inline("📋 Show Queue", data="show_q_active"),
                      Button.inline("❌ Cancel", data="cancel_task")]]

    if DOWNLOAD_SEMAPHORE.locked():
        await event.respond("⏳ **Waiting for a free download slot...**", buttons=task_buttons)

    async with DOWNLOAD_SEMAPHORE:
        if "instagram.com" in url:
            await run_instagram_download(event, url, task_buttons)
        else:
            await run_standard_download(event, url, quality, task_buttons)


async def run_instagram_download(event, url, task_buttons):
    user_id = event.sender_id
    status_msg = await event.respond("📸 **Instagram link detected** — fetching...", buttons=task_buttons)

    timestamp = int(time.time())
    work_dir = f"downloads/ig_{user_id}_{timestamp}"
    os.makedirs(work_dir, exist_ok=True)

    try:
        files = await asyncio.get_event_loop().run_in_executor(
            None, lambda: download_instagram_content(url, work_dir)
        )
        if not files:
            await safe_edit(status_msg, "❌ **Instagram download failed.** "
                                         "The post may be private, age-restricted, or unavailable.")
            return

        total_size = sum(os.path.getsize(f) for f in files)
        await safe_edit(status_msg, f"📤 **Uploading {len(files)} item(s)...**\nSize: {format_size(total_size)}",
                         buttons=task_buttons)

        sent, failed = await send_media_batch(event, files, status_msg)
        if sent:
            update_stats(user_id, sent, total_size)
        await status_msg.delete()
        logging.info(f"Instagram task finished for user {user_id}: {sent} sent, {failed} failed")

    except Exception as e:
        logging.error(f"Instagram error for user {user_id}: {e}")
        await safe_edit(status_msg, f"❌ **Error:** {str(e)[:150]}")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def run_standard_download(event, url, quality, task_buttons):
    user_id = event.sender_id
    mode = get_user_data(user_id).get('mode', 'normal')
    mode_text = "⚡ Aria2" if mode == 'fast' else "🐢 Native"
    status_msg = await event.respond(f"🏎️ **Starting Engine ({mode_text})...**", buttons=task_buttons)

    timestamp = int(time.time())
    out_tmpl = f"downloads/{user_id}_{timestamp}_%(id)s.%(ext)s"

    is_audio = quality == 'audio'
    target_height = None
    if is_audio:
        fmt = "bestaudio/best"
    elif quality == 'best':
        fmt = "bestvideo+bestaudio/best"
    else:
        target_height = int(quality)
        fmt = f"bestvideo[height<={target_height}]+bestaudio/best[height<={target_height}]/best"

    last_update = [0]
    start_time = [time.time()]

    def dl_progress(d):
        if TASK_CANCEL.get(user_id):
            raise yt_dlp.utils.DownloadError("Cancelled")
        if d['status'] == 'downloading' and time.time() - last_update[0] > 3:
            try:
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                current = d.get('downloaded_bytes', 0)
                text = f"📥 **Downloading ({mode_text})**\n{get_progress_bar(current, total, start_time[0])}"
                if not client.loop.is_closed():
                    asyncio.run_coroutine_threadsafe(safe_edit(status_msg, text, buttons=task_buttons), client.loop)
                last_update[0] = time.time()
            except Exception:
                pass

    async def try_download(ua_type):
        ua = DESKTOP_UA if ua_type == "desktop" else MOBILE_UA
        ydl_opts = {
            'outtmpl': out_tmpl,
            'format': fmt,
            'quiet': True,
            'progress_hooks': [dl_progress],
            'noplaylist': True,
            'ignoreerrors': True,
            'no_warnings': True,
            'restrictfilenames': True,
            'nocheckcertificate': True,
            'user_agent': ua,
            'referer': 'https://www.google.com/',
            'noprogress': True,
            'verbose': False,
            'concurrent_fragment_downloads': 4,
            'retries': 5,
            'fragment_retries': 5,
        }
        if is_audio:
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }]
        if mode == 'fast':
            ydl_opts['external_downloader'] = 'aria2c'
            ydl_opts['external_downloader_args'] = [
                '-x', '16', '-s', '32', '-k', '1M',
                '--file-allocation=none',
                '--max-connection-per-server=16',
                '--min-split-size=1M',
                '--summary-interval=0',
                '--console-log-level=warn',
            ]

        def start_dl():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(url, download=True)

        await asyncio.get_event_loop().run_in_executor(None, start_dl)

    def find_output():
        files = glob.glob(f"downloads/{user_id}_{timestamp}_*")
        return next((f for f in files if not f.endswith('.part')), None)

    status_msg_local = status_msg
    try:
        logging.info(f"Starting task for user {user_id} (mode={mode}, quality={quality})")
        final_file = None
        for ua_type, delay in (("desktop", 0), ("mobile", 3), ("desktop", 8)):
            if delay:
                status_msg_local = await safe_edit(status_msg_local, f"⚠️ **Retrying ({ua_type})...**", buttons=task_buttons)
                await asyncio.sleep(delay)
            try:
                await try_download(ua_type)
            except Exception as e:
                if "Cancelled" in str(e):
                    raise
                logging.warning(f"Attempt with {ua_type} UA failed: {e}")
            final_file = find_output()
            if final_file and os.path.exists(final_file):
                break

        if not (final_file and os.path.exists(final_file)):
            await safe_edit(status_msg_local, "❌ **Download failed** after multiple attempts.")
            logging.warning(f"Task failed for user {user_id}")
            return

        if not is_audio:
            if target_height:
                final_file = await transcode_video(final_file, target_height, status_msg_local)
            else:
                final_file = await ensure_faststart(final_file, status_msg_local)

        ext = os.path.splitext(final_file)[1] or ('.mp3' if is_audio else '.mp4')
        safe_name = f"Media_{timestamp}_{random.randint(1000, 9999)}{ext}"
        os.rename(final_file, safe_name)
        final_file = safe_name

        file_size = os.path.getsize(final_file)
        if file_size > TELEGRAM_FILE_LIMIT:
            await safe_edit(status_msg_local,
                             f"⚠️ **File is {format_size(file_size)}** — over Telegram's 2GB limit. "
                             f"Try a lower quality.")
            os.remove(final_file)
            return

        status_msg_local = await safe_edit(status_msg_local, f"📤 **Uploading...**\nSize: {format_size(file_size)}",
                                            buttons=task_buttons)

        thumb_path = None
        if is_audio:
            duration, _, _ = await probe_media(final_file)
            attributes = [DocumentAttributeAudio(duration=duration, title="Downloaded Audio")]
        else:
            attributes = await build_video_attributes(final_file)
            thumb_path = await make_thumbnail(final_file)

        async def upload_progress(current, total):
            if TASK_CANCEL.get(user_id):
                raise Exception("Cancelled")
            if time.time() - last_update[0] > 8:
                text = f"📤 **Uploading**\n{get_progress_bar(current, total, start_time[0])}"
                await safe_edit(status_msg_local, text, buttons=task_buttons)
                last_update[0] = time.time()

        await client.send_file(
            event.chat_id,
            final_file,
            caption="✅ **Download Complete**",
            thumb=thumb_path if thumb_path and os.path.exists(thumb_path) else None,
            progress_callback=upload_progress,
            attributes=attributes,
            supports_streaming=True,
            part_size_kb=512,  # Telegram's documented maximum chunk size
        )

        update_stats(user_id, 1, file_size)
        await status_msg_local.delete()
        if os.path.exists(final_file):
            os.remove(final_file)
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)
        logging.info(f"Task finished for user {user_id}")

    except Exception as e:
        if "Cancelled" in str(e):
            await safe_edit(status_msg_local, "🛑 **Cancelled.**")
        else:
            logging.error(f"Download error for user {user_id}: {e}")
            await safe_edit(status_msg_local, f"❌ **Error:** {str(e)[:150]}")


# ============================================================================
# QUEUE WORKER
# ============================================================================

async def queue_worker(event, user_id):
    ud = get_user_data(user_id)
    if ud.get('is_busy', False):
        return
    ud['is_busy'] = True
    try:
        while True:
            queue = get_user_data(user_id).get('queue', [])
            if not queue:
                break
            task = queue.pop(0)
            try:
                await run_download(event, task['url'], task['quality'])
            except Exception as e:
                logging.error(f"Queue task error for user {user_id}: {e}")
            await asyncio.sleep(1)
    finally:
        get_user_data(user_id)['is_busy'] = False
        await event.respond("✅ **All tasks in queue finished.**")


# ============================================================================
# ENTRYPOINT
# ============================================================================

async def main():
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    logging.info(f"✅ Bot running as @{me.username} "
                 f"(concurrent slots: {MAX_CONCURRENT_DOWNLOADS}, admins: {len(ADMIN_IDS)})")

    loop = asyncio.get_event_loop()
    loop.create_task(autosave_loop())

    async def shutdown(sig_name):
        logging.info(f"Received {sig_name}; saving state before exit...")
        try:
            await asyncio.wait_for(persist_state(push=True), timeout=25)
        except Exception as e:
            logging.warning(f"Shutdown persistence failed: {e}")
        await client.disconnect()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s.name)))
        except NotImplementedError:
            pass  # not supported on this platform (e.g. Windows)

    await client.run_until_disconnected()


if __name__ == "__main__":
    _state = load_state()
    AUTHORIZED_USERS.update(_state.get('authorized_users', []))
    STATS.update(_state.get('stats', {}))
    for _uid_str, _mode in _state.get('user_modes', {}).items():
        try:
            get_user_data(int(_uid_str))['mode'] = _mode
        except ValueError:
            pass

    os.makedirs("downloads", exist_ok=True)
    print("✅ Bot starting (deps pinned + async media probing + persistence + queue fixes)...")
    client.loop.run_until_complete(main())
