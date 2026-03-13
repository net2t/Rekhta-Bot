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
    # Primary account — REQUIRED
    DD_NICK     = os.getenv("DD_LOGIN_EMAIL", "").strip()   # DamaDam username/nick
    DD_PASS     = os.getenv("DD_LOGIN_PASS",  "").strip()
    # Backup account — optional, used if primary login fails
    DD_NICK2    = os.getenv("DD_LOGIN_EMAIL2", "").strip()
    DD_PASS2    = os.getenv("DD_LOGIN_PASS2",  "").strip()

    # ── Google Sheets ──────────────────────────────────────────────────────────
    SHEET_ID         = os.getenv("DD_SHEET_ID", "").strip()
    CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json").strip()
    CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()  # JSON string alternative

    # ── Browser ────────────────────────────────────────────────────────────────
    CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "").strip()
    HEADLESS          = os.getenv("DD_HEADLESS", "1").strip().lower() in {"1", "true", "yes"}
    PAGE_LOAD_TIMEOUT = int(os.getenv("DD_PAGE_LOAD_TIMEOUT", "15") or "15")

    # ── Cookie file for session persistence ────────────────────────────────────
    COOKIE_FILE = str(SCRIPT_DIR / "damadam_cookies.pkl")

    # ── Run Flags ──────────────────────────────────────────────────────────────
    DRY_RUN     = os.getenv("DD_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
    DEBUG       = os.getenv("DD_DEBUG",   "0").strip() == "1"
    MAX_PROFILES = int(os.getenv("DD_MAX_PROFILES", "0") or "0")  # 0 = unlimited

    # ── GitHub Actions detection ───────────────────────────────────────────────
    IS_CI = bool(os.getenv("GITHUB_ACTIONS"))

    # ── URLs ───────────────────────────────────────────────────────────────────
    BASE_URL  = "https://damadam.pk"
    LOGIN_URL = "https://damadam.pk/login/"
    HOME_URL  = "https://damadam.pk/"

    # ── Message Mode Settings ──────────────────────────────────────────────────
    MAX_POST_PAGES    = int(os.getenv("DD_MAX_POST_PAGES", "4") or "4")
    MSG_DELAY_SECONDS = float(os.getenv("DD_MSG_DELAY_SECONDS", "3") or "3")

    # ── Post Mode Settings ─────────────────────────────────────────────────────
    # Hard cooldown between posts: DamaDam enforces ~2min 10sec
    POST_COOLDOWN_SECONDS  = int(os.getenv("DD_POST_COOLDOWN_SECONDS", "135") or "135")
    POST_CAPTION_MAX_LEN   = int(os.getenv("DD_POST_CAPTION_MAX_LEN", "300") or "300")
    POST_TAGS_MAX_LEN      = int(os.getenv("DD_POST_TAGS_MAX_LEN", "120") or "120")
    POST_MAX_REPEAT_CHARS  = int(os.getenv("DD_POST_MAX_REPEAT_CHARS", "6") or "6")

    # ── Rekhta Mode Settings ───────────────────────────────────────────────────
    REKHTA_URL        = os.getenv("DD_REKHTA_URL", "https://www.rekhta.org/shayari-image")
    REKHTA_MAX_SCROLLS = int(os.getenv("DD_REKHTA_MAX_SCROLLS", "6") or "6")

    # ── Image Download Settings ────────────────────────────────────────────────
    IMAGE_DOWNLOAD_TIMEOUT = int(os.getenv("DD_IMAGE_DOWNLOAD_TIMEOUT", "90") or "90")
    IMAGE_DOWNLOAD_RETRIES = int(os.getenv("DD_IMAGE_DOWNLOAD_RETRIES", "3")  or "3")

    # ── Logging ────────────────────────────────────────────────────────────────
    LOG_DIR = SCRIPT_DIR / "logs"
    LOG_DIR.mkdir(exist_ok=True)

    # ── Bot Version ────────────────────────────────────────────────────────────
    VERSION = "2.0.0"

    # ── Signature appended to every post caption ───────────────────────────────
    # Leave empty in .env to disable. E.g: DD_POST_SIGNATURE=🌹
    POST_SIGNATURE = os.getenv("DD_POST_SIGNATURE", "").strip()


    # ════════════════════════════════════════════════════════════════════════════
    #  SHEET NAMES  (these must match the tab names in your Google Sheet)
    # ════════════════════════════════════════════════════════════════════════════
    SHEET_MSG_LIST   = "MsgList"       # Targets for Message Mode
    SHEET_POST_QUEUE = "PostQueue"     # Queue for Post Mode
    SHEET_MASTER_LOG = "MasterLog"     # All activity log
    SHEET_INBOX      = "InboxQueue"    # Inbox reply queue


    # ════════════════════════════════════════════════════════════════════════════
    #  COLUMN DEFINITIONS — Single source of truth for every sheet
    #
    #  Format: list of column header strings, in left-to-right order.
    #  The index (0-based) of each name IS the column number.
    #  These are used by Setup Mode to create/format sheets, and by all modes
    #  to locate the right column regardless of column order changes.
    # ════════════════════════════════════════════════════════════════════════════

    # ── MsgList sheet columns ──────────────────────────────────────────────────
    #  Col A: MODE      — type of target (Nick / URL)
    #  Col B: NAME      — display name (optional, for your reference)
    #  Col C: NICK      — DamaDam nickname or profile URL
    #  Col D: CITY      — scraped city (read-only reference)
    #  Col E: POSTS     — scraped posts count (read-only reference)
    #  Col F: FOLLOWERS — scraped followers count (read-only reference)
    #  Col G: GENDER    — scraped gender icon (read-only reference)
    #  Col H: MESSAGE   — message template to send (use {{name}}, {{city}}, etc.)
    #  Col I: STATUS    — Pending / Done / Skipped / Failed
    #  Col J: NOTES     — result notes (set by bot)
    #  Col K: RESULT    — URL of the post where message was sent
    MSG_LIST_COLS = [
        "MODE",      # 0 — A
        "NAME",      # 1 — B
        "NICK",      # 2 — C  ← was NICK/URL
        "CITY",      # 3 — D  (reference only)
        "POSTS",     # 4 — E  (reference only)
        "FOLLOWERS", # 5 — F  (reference only)
        "GENDER",    # 6 — G  (reference only)
        "MESSAGE",   # 7 — H
        "STATUS",    # 8 — I
        "NOTES",     # 9 — J
        "RESULT",    # 10 — K  ← was RESULT URL
    ]

    # ── PostQueue sheet columns ────────────────────────────────────────────────
    #  Col A: STATUS   — Pending / Done / Failed / Skipped / Repeating
    #  Col B: TYPE     — image / text
    #  Col C: TITLE    — English title / first line (reference)
    #  Col D: URDU     — Urdu caption (used as post body)
    #  Col E: IMG_LINK — Full image URL from Rekhta
    #  Col F: POET     — Poet name
    #  Col G: POST_URL — Posted URL (set by bot after success)
    #  Col H: ADDED    — Timestamp when row was scraped (set by Rekhta mode)
    #  Col I: NOTES    — Error notes (set by bot)
    POST_QUEUE_COLS = [
        "STATUS",    # 0 — A
        "TYPE",      # 1 — B
        "TITLE",     # 2 — C
        "URDU",      # 3 — D
        "IMG_LINK",  # 4 — E
        "POET",      # 5 — F
        "POST_URL",  # 6 — G
        "ADDED",     # 7 — H
        "NOTES",     # 8 — I
    ]

    # ── MasterLog sheet columns ────────────────────────────────────────────────
    MASTER_LOG_COLS = [
        "TIMESTAMP", # 0
        "MODE",      # 1
        "ACTION",    # 2
        "NICK",      # 3
        "URL",       # 4
        "STATUS",    # 5
        "DETAILS",   # 6
    ]

    # ── InboxQueue sheet columns ───────────────────────────────────────────────
    INBOX_COLS = [
        "NICK",          # 0
        "NAME",          # 1
        "LAST_MSG",      # 2
        "MY_REPLY",      # 3
        "STATUS",        # 4
        "TIMESTAMP",     # 5
        "NOTES",         # 6
    ]

    @classmethod
    def validate(cls):
        """Validate required settings are present. Exits if critical values missing."""
        errors = []
        if not cls.DD_NICK:
            errors.append("DD_LOGIN_EMAIL (DamaDam username) is required")
        if not cls.DD_PASS:
            errors.append("DD_LOGIN_PASS is required")
        if not cls.SHEET_ID:
            errors.append("DD_SHEET_ID (Google Sheets ID) is required")
        # Check credentials: either a JSON string or a file must exist
        has_json = bool(cls.CREDENTIALS_JSON)
        has_file = Path(cls.CREDENTIALS_FILE).exists() or (SCRIPT_DIR / cls.CREDENTIALS_FILE).exists()
        if not has_json and not has_file:
            errors.append(f"Google credentials not found. Need {cls.CREDENTIALS_FILE} or GOOGLE_CREDENTIALS_JSON env var.")
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
        full = SCRIPT_DIR / cls.CREDENTIALS_FILE
        return str(full)
