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
    DISABLE_IMAGES    = os.getenv("DD_DISABLE_IMAGES", "1").strip().lower() in {"1", "true", "yes"}
    PAGE_LOAD_TIMEOUT = int(os.getenv("DD_PAGE_LOAD_TIMEOUT", "60") or "60")

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
    SHEET_POST_QUE  = "PostQue"    # Post content queue (populated by Rekhta mode)

    # Log sheets    — bot writes results here after each action
    SHEET_POST_LOG  = "PostLog"    # Every post created (history per post)

    # Master log    — one row per any action across all modes
    SHEET_LOGS      = "Logs"

    # Run log       — one row per bot run (mode, counts, duration)
    SHEET_RUN_LOG   = "RunLog"

    # Scrape state  — pagination cursor so Mode 1 resumes instead of re-scanning
    SHEET_SCRAPE_STATE = "ScrapeState"

    # Dashboard     — summary/analysis (formulas only, bot never writes here)
    SHEET_DASHBOARD = "Dashboard"

    # ── Keep old names as aliases so existing code doesn't break ──────────────
    SHEET_MASTER_LOG = SHEET_LOGS          # backwards compat alias
    SHEET_POST_QUEUE = SHEET_POST_QUE      # backwards compat alias

    # ════════════════════════════════════════════════════════════════════════════
    #  COLUMN DEFINITIONS — single source of truth for every sheet
    # ════════════════════════════════════════════════════════════════════════════

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

    # ── RunLog — one row per complete bot run ─────────────────────────────────
    RUN_LOG_COLS = [
        "TIMESTAMP",  # A  When the run started (PKT)
        "MODE",       # B  Which mode was run (rekhta/post)
        "STATUS",     # C  Done / Failed / Stopped
        "ADDED",      # D  Items added    (Rekhta: new rows in PostQue)
        "POSTED",     # E  Posts created  (Post mode)
        "SENT",       # F  Messages sent  (legacy field)
        "FAILED",     # G  Failures
        "SKIPPED",    # H  Skipped rows
        "DURATION",   # I  How long the run took (seconds)
        "NOTES",      # J  Extra info or error summary
    ]

    # ── ScrapeState — key/value store for pagination cursors ──────────────────
    SCRAPE_STATE_COLS = [
        "KEY",        # A  State key (e.g. "rekhta_last_page")
        "VALUE",      # B  State value
        "UPDATED",    # C  When this value was last written
    ]

    # ── Backwards compat aliases for column lists ──────────────────────────────
    POST_QUEUE_COLS = POST_QUE_COLS
    MASTER_LOG_COLS = LOGS_COLS

    # ════════════════════════════════════════════════════════════════════════════
    #  All sheets in setup order
    # ════════════════════════════════════════════════════════════════════════════
    ALL_SHEETS = {
        SHEET_POST_QUE:     POST_QUE_COLS,
        SHEET_POST_LOG:     POST_LOG_COLS,
        SHEET_LOGS:         LOGS_COLS,
        SHEET_RUN_LOG:      RUN_LOG_COLS,
        SHEET_SCRAPE_STATE: SCRAPE_STATE_COLS,
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
