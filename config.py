"""
config.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━
All configuration, constants, sheet names, and column definitions.
All settings come from environment variables (or .env file).
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env file if present ──────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.absolute()
env_path = SCRIPT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)


class Config:
    """All runtime settings, loaded from environment variables."""

    # ── DamaDam Credentials ────────────────────────────────────────────────────
    DD_NICK  = os.getenv("DD_LOGIN_EMAIL",  "").strip()  # DamaDam username/nick
    DD_PASS  = os.getenv("DD_LOGIN_PASS",   "").strip()
    DD_NICK2 = os.getenv("DD_LOGIN_EMAIL2", "").strip()  # Backup account (optional)
    DD_PASS2 = os.getenv("DD_LOGIN_PASS2",  "").strip()

    # ── Google Sheets ──────────────────────────────────────────────────────────
    SHEET_ID         = os.getenv("DD_SHEET_ID", "").strip()
    CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json").strip()
    CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()

    # ── Browser ────────────────────────────────────────────────────────────────
    CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "").strip()
    HEADLESS          = os.getenv("DD_HEADLESS", "1").strip().lower() in {"1", "true", "yes"}
    PAGE_LOAD_TIMEOUT = int(os.getenv("DD_PAGE_LOAD_TIMEOUT", "15") or "15")

    # ── Cookie file for session persistence ────────────────────────────────────
    COOKIE_FILE = str(SCRIPT_DIR / "damadam_cookies.pkl")

    # ── Run Flags ──────────────────────────────────────────────────────────────
    DRY_RUN      = os.getenv("DD_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
    DEBUG        = os.getenv("DD_DEBUG",   "0").strip() == "1"
    MAX_PROFILES = int(os.getenv("DD_MAX_PROFILES", "0") or "0")

    # ── GitHub Actions detection ───────────────────────────────────────────────
    IS_CI = bool(os.getenv("GITHUB_ACTIONS"))

    # ── URLs ───────────────────────────────────────────────────────────────────
    BASE_URL  = "https://damadam.pk"
    LOGIN_URL = "https://damadam.pk/login/"
    HOME_URL  = "https://damadam.pk/"

    # ── Message Mode ──────────────────────────────────────────────────────────
    MAX_POST_PAGES    = int(os.getenv("DD_MAX_POST_PAGES",    "4") or "4")
    MSG_DELAY_SECONDS = float(os.getenv("DD_MSG_DELAY_SECONDS", "3") or "3")

    # ── Post Mode ─────────────────────────────────────────────────────────────
    POST_COOLDOWN_SECONDS = int(os.getenv("DD_POST_COOLDOWN_SECONDS", "135") or "135")
    POST_CAPTION_MAX_LEN  = int(os.getenv("DD_POST_CAPTION_MAX_LEN",  "300") or "300")
    POST_TAGS_MAX_LEN     = int(os.getenv("DD_POST_TAGS_MAX_LEN",     "120") or "120")
    POST_MAX_REPEAT_CHARS = int(os.getenv("DD_POST_MAX_REPEAT_CHARS",   "6") or "6")
    POST_SIGNATURE        = os.getenv("DD_POST_SIGNATURE", "").strip()

    # ── Rekhta Mode ───────────────────────────────────────────────────────────
    REKHTA_URL         = os.getenv("DD_REKHTA_URL", "https://www.rekhta.org/shayari-image")
    REKHTA_MAX_SCROLLS = int(os.getenv("DD_REKHTA_MAX_SCROLLS", "6") or "6")

    # ── Image Download ────────────────────────────────────────────────────────
    IMAGE_DOWNLOAD_TIMEOUT = int(os.getenv("DD_IMAGE_DOWNLOAD_TIMEOUT", "90") or "90")
    IMAGE_DOWNLOAD_RETRIES = int(os.getenv("DD_IMAGE_DOWNLOAD_RETRIES",  "3") or "3")

    # ── Logging ────────────────────────────────────────────────────────────────
    LOG_DIR = SCRIPT_DIR / "logs"
    LOG_DIR.mkdir(exist_ok=True)

    # ── Bot Version ────────────────────────────────────────────────────────────
    VERSION = "2.1.0"

    # ════════════════════════════════════════════════════════════════════════════
    #  SHEET NAMES
    #  These must match the tab names in your Google Spreadsheet exactly.
    # ════════════════════════════════════════════════════════════════════════════

    # Queue sheets  — bot reads from these to know what to do
    SHEET_MSG_QUE   = "MsgQue"     # Message targets queue
    SHEET_POST_QUE  = "PostQue"    # Post content queue (populated by Rekhta mode)
    SHEET_INBOX_QUE = "InboxQue"   # Inbox conversations + pending replies

    # Log sheets    — bot writes results here after each action
    SHEET_MSG_LOG   = "MsgLog"     # Every message sent (history per target)
    SHEET_POST_LOG  = "PostLog"    # Every post created (history per post)
    SHEET_INBOX_LOG = "InboxLog"   # Every inbox conversation + activity entry

    # Master log    — one row per any action across all modes
    SHEET_LOGS      = "Logs"

    # Dashboard     — summary/analysis (formulas only, bot never writes here)
    SHEET_DASHBOARD = "Dashboard"

    # ── Keep old names as aliases so existing code doesn't break ──────────────
    # (removed after full migration is done)
    SHEET_MASTER_LOG = SHEET_LOGS          # backwards compat alias
    SHEET_MSG_LIST   = SHEET_MSG_QUE       # backwards compat alias
    SHEET_POST_QUEUE = SHEET_POST_QUE      # backwards compat alias
    SHEET_INBOX      = SHEET_INBOX_QUE     # backwards compat alias

    # ════════════════════════════════════════════════════════════════════════════
    #  COLUMN DEFINITIONS — single source of truth for every sheet
    # ════════════════════════════════════════════════════════════════════════════

    # ── MsgQue — message targets queue ────────────────────────────────────────
    #  You fill this in. Bot reads it, sends messages, updates STATUS/NOTES/RESULT.
    MSG_QUE_COLS = [
        "MODE",       # A  Nick / URL
        "NAME",       # B  Display name (your reference)
        "NICK",       # C  DamaDam username or profile URL
        "CITY",       # D  Scraped city       (read-only reference)
        "POSTS",      # E  Scraped post count  (read-only reference)
        "FOLLOWERS",  # F  Scraped followers   (read-only reference)
        "GENDER",     # G  Scraped gender      (read-only reference)
        "MESSAGE",    # H  Template text — supports {{name}}, {{city}} placeholders
        "STATUS",     # I  Pending → Done / Skipped / Failed
        "NOTES",      # J  Set by bot
        "RESULT",     # K  URL of post where message was sent
        "SENT_MSG",   # L  Actual resolved message that was sent (set by bot)
    ]

    # ── MsgLog — one row per message sent ─────────────────────────────────────
    #  Bot appends here after every successful or failed message attempt.
    MSG_LOG_COLS = [
        "TIMESTAMP",  # A  PKT timestamp
        "NICK",       # B  Target username
        "NAME",       # C  Display name
        "MESSAGE",    # D  Message text that was sent
        "POST_URL",   # E  URL of post the message was sent on
        "STATUS",     # F  Sent / Failed / Skipped
        "NOTES",      # G  Error or extra detail
    ]

    # ── PostQue — post content queue ──────────────────────────────────────────
    #  Populated by Rekhta mode. Bot reads it and creates posts.
    POST_QUE_COLS = [
        "STATUS",     # A  Pending → Done / Failed / Skipped / Repeating
        "TYPE",       # B  image / text
        "TITLE",      # C  Roman Urdu first line (reference)
        "URDU",       # D  Urdu caption — use =GOOGLETRANSLATE() formula here
        "IMG_LINK",   # E  Full image URL from Rekhta
        "POET",       # F  Poet name
        "POST_URL",   # G  Filled by bot after successful post
        "ADDED",      # H  Timestamp when row was scraped
        "NOTES",      # I  Error details set by bot
    ]

    # ── PostLog — one row per post created ────────────────────────────────────
    #  Bot appends here after every post attempt.
    POST_LOG_COLS = [
        "TIMESTAMP",  # A  PKT timestamp
        "TYPE",       # B  image / text
        "POET",       # C  Poet name
        "TITLE",      # D  Roman Urdu first line
        "POST_URL",   # E  URL of the created post
        "IMG_LINK",   # F  Source image URL
        "STATUS",     # G  Posted / Failed / Repeating / Skipped
        "NOTES",      # H  Error or extra detail
    ]

    # ── InboxQue — inbox conversations + reply queue ──────────────────────────
    #  Bot syncs new conversations here. You fill MY_REPLY. Bot sends it.
    INBOX_QUE_COLS = [
        "TID",        # A  DamaDam user ID (tid from button value — never changes)
        "NICK",       # B  DamaDam username
        "NAME",       # C  Display name
        "TYPE",       # D  1ON1 / POST / MEHFIL / UNKNOWN
        "LAST_MSG",   # E  Last message received
        "MY_REPLY",   # F  Your reply text — bot sends this when STATUS=Pending
        "STATUS",     # G  Pending → Done / Failed / NoReply
        "UPDATED",    # H  Timestamp of last sync
        "NOTES",      # I  Set by bot
    ]

    # ── InboxLog — full inbox + activity history ───────────────────────────────
    #  One row per inbox event or activity item. Complete history.
    INBOX_LOG_COLS = [
        "TIMESTAMP",  # A  PKT timestamp
        "TID",        # B  DamaDam user ID
        "NICK",       # C  Username
        "TYPE",       # D  1ON1 / POST / MEHFIL / ACTIVITY
        "DIRECTION",  # E  IN / OUT / ACTIVITY
        "MESSAGE",    # F  Message text or activity description
        "CONV_URL",   # G  Link to the conversation or post
        "STATUS",     # H  Received / Sent / Failed / Logged
    ]

    # ── Logs — master log (one row per any bot action) ─────────────────────────
    LOGS_COLS = [
        "TIMESTAMP",  # A
        "MODE",       # B
        "ACTION",     # C
        "NICK",       # D
        "URL",        # E
        "STATUS",     # F
        "DETAILS",    # G
    ]

    # ── Backwards compat aliases for column lists ──────────────────────────────
    MSG_LIST_COLS   = MSG_QUE_COLS
    POST_QUEUE_COLS = POST_QUE_COLS
    MASTER_LOG_COLS = LOGS_COLS
    INBOX_COLS      = INBOX_QUE_COLS

    # ════════════════════════════════════════════════════════════════════════════
    #  All sheets in setup order
    # ════════════════════════════════════════════════════════════════════════════
    ALL_SHEETS = {
        SHEET_MSG_QUE:   MSG_QUE_COLS,
        SHEET_POST_QUE:  POST_QUE_COLS,
        SHEET_INBOX_QUE: INBOX_QUE_COLS,
        SHEET_MSG_LOG:   MSG_LOG_COLS,
        SHEET_POST_LOG:  POST_LOG_COLS,
        SHEET_INBOX_LOG: INBOX_LOG_COLS,
        SHEET_LOGS:      LOGS_COLS,
        # Dashboard has no fixed columns — it's formula-based, created empty
    }

    @classmethod
    def validate(cls):
        """Validate required settings. Exits if critical values are missing."""
        errors = []
        if not cls.DD_NICK:
            errors.append("DD_LOGIN_EMAIL (DamaDam username) is required")
        if not cls.DD_PASS:
            errors.append("DD_LOGIN_PASS is required")
        if not cls.SHEET_ID:
            errors.append("DD_SHEET_ID (Google Sheets ID) is required")
        has_json = bool(cls.CREDENTIALS_JSON)
        has_file = (Path(cls.CREDENTIALS_FILE).exists()
                    or (SCRIPT_DIR / cls.CREDENTIALS_FILE).exists())
        if not has_json and not has_file:
            errors.append(
                f"Google credentials not found. "
                f"Need {cls.CREDENTIALS_FILE} or GOOGLE_CREDENTIALS_JSON env var."
            )
        if errors:
            print("=" * 60)
            for e in errors:
                print(f"[CONFIG ERROR] {e}")
            print("=" * 60)
            sys.exit(1)
        return True

    @classmethod
    def get_credentials_path(cls):
        """Return absolute path to credentials.json."""
        p = Path(cls.CREDENTIALS_FILE)
        if p.is_absolute() and p.exists():
            return str(p)
        return str(SCRIPT_DIR / cls.CREDENTIALS_FILE)
