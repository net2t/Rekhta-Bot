"""
DamaDam Bot V2.0.0 - Single File Complete Version
Clean, Organized, Multi-Mode Bot

Usage:
    python main.py --mode msg --max-profiles 10
    python main.py --mode post
    python main.py --mode inbox

Modes:
    msg   - Send personal messages (Phase 1)
    post  - Create new posts (Phase 2)
    inbox - Monitor inbox & reply (Phase 3)
"""

import time
import os
import sys
import re
import pickle
import argparse
import tempfile
import mimetypes
import urllib.request
import urllib.error
import socket
import logging
import random
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, urlparse, parse_qs

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from gspread.utils import rowcol_to_a1

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

try:
    logging.getLogger("dotenv").setLevel(logging.ERROR)
    logging.getLogger("dotenv.main").setLevel(logging.ERROR)
except Exception:
    pass

load_dotenv(override=False)

_rich_force = os.getenv("DD_RICH_FORCE", "0").strip().lower() in {"1", "true", "yes", "y"}
_rich_color = os.getenv("DD_RICH_COLOR_SYSTEM", "auto").strip() or "auto"

console = Console(
    force_terminal=_rich_force,
    color_system=_rich_color,
)
VERSION = "2.1.0"

def _sheet_url(sheet_id: str) -> str:
    sid = (sheet_id or "").strip()
    if not sid:
        return ""
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit"

def _print_sheet_context(logger: "Logger"):
    try:
        url = _sheet_url(Config.SHEET_ID)
        if url:
            logger.info("📎 Sheet Link")
    except Exception:
        pass

def run_logs_mode():
    logger = Logger("logs")
    logger.info("=" * 70)
    logger.info(f"DamaDam Bot V{VERSION} - LOGS")
    logger.info("=" * 70)

    try:
        logger.info(f"📁 Log folder: {Config.LOG_DIR.resolve()}")
        try:
            files = sorted(Config.LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        except Exception:
            files = []

        if files:
            logger.info("\nRecent local log files:")
            for p in files[:10]:
                try:
                    ts = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts = ""
                logger.info(f"- {p.name} ({ts})")
        else:
            logger.info("No local log files found.")

        sheets_mgr = SheetsManager(logger)
        if not sheets_mgr.connect():
            logger.warning("Sheets not available (cannot show ActivityLog/ConversationLog)")
            return

        _print_sheet_context(logger)

        def _tail_sheet(sheet_name: str, limit: int = 20) -> None:
            sh = sheets_mgr.get_sheet(Config.SHEET_ID, sheet_name, create_if_missing=False)
            if not sh:
                # Try legacy names
                legacy = {"MasterLog": "Logs", "MsgQueue": "MsgHistory",
                          "PostQueueLog": "PostHistory", "InboxQueue": "Inbox"}
                fallback = legacy.get(sheet_name)
                if fallback:
                    sh = sheets_mgr.get_sheet(Config.SHEET_ID, fallback, create_if_missing=False)
            if not sh:
                logger.warning(f"Sheet not found: {sheet_name}")
                return
            sheets_mgr.api_calls += 1
            rows = sh.get_all_values()
            if not rows or len(rows) <= 1:
                logger.info(f"{sheet_name}: (empty)")
                return

            hdr = rows[0]
            body = rows[1:]
            tail = body[-limit:]
            logger.info(f"\n{sheet_name} (last {min(limit, len(body))} rows)")
            logger.info(" | ".join([(h or "").strip() for h in hdr]))
            for r in tail:
                logger.info(" | ".join([(c or "").strip() for c in r]))

        _tail_sheet("MasterLog", limit=20)
        _tail_sheet("MsgQueue", limit=10)
        _tail_sheet("PostQueueLog", limit=10)
    finally:
        logger.info(f"\n📝 Log: {logger.log_file}")


def run_setup_mode():
    logger = Logger("setup")
    logger.info("=" * 70)
    logger.info(f"DamaDam Bot V{VERSION} - SETUP")
    logger.info("=" * 70)

    sheets_mgr = SheetsManager(logger)
    if not sheets_mgr.connect():
        logger.error("Sheets connection failed")
        return

    _print_sheet_context(logger)

    required_sheets = [
        "MsgList",       # MSG targets
        "MsgQueue",      # MSG job queue (pending/done/failed per run)
        "PostQueue",     # POST jobs queue
        "PostQueueLog",  # POST results log
        "InboxQueue",    # INBOX conversations + replies
        "MasterLog",     # Single log for ALL modes
    ]

    for name in required_sheets:
        sheet = sheets_mgr.get_sheet(Config.SHEET_ID, name, create_if_missing=True)
        if sheet:
            sheets_mgr.style_sheet(sheet)
            logger.success(f"✅ Sheet ready: {name}")
        else:
            logger.warning(f"⚠️ Sheet missing: {name}")

    logger.info(f"\n📝 Log: {logger.log_file}")

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Centralized bot configuration"""

    # Authentication
    LOGIN_EMAIL = os.getenv("DD_LOGIN_EMAIL", "0utLawZ")
    LOGIN_PASS = os.getenv("DD_LOGIN_PASS", "asdasd")
    LOGIN_EMAIL2 = os.getenv("DD_LOGIN_EMAIL2", "").strip()
    LOGIN_PASS2 = os.getenv("DD_LOGIN_PASS2", "").strip()
    COOKIE_FILE = os.getenv("COOKIE_FILE", "damadam_cookies.pkl")

    # Google Sheets
    SHEET_ID = os.getenv("DD_SHEET_ID", "1xph0dra5-wPcgMXKubQD7A2CokObpst7o2rWbDA10t8")
    PROFILES_SHEET_ID = os.getenv("DD_PROFILES_SHEET_ID", "")
    CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")

    # Browser
    CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "chromedriver.exe")
    HEADLESS = os.getenv("DD_HEADLESS", "1").strip().lower() in {"1", "true", "yes", "y"}

    # Bot Settings
    DEBUG = os.getenv("DD_DEBUG", "0") == "1"
    DRY_RUN = os.getenv("DD_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"}
    MAX_PROFILES = int(os.getenv("DD_MAX_PROFILES", "0"))
    MAX_POST_PAGES = int(os.getenv("DD_MAX_POST_PAGES", "4") or "4")
    POST_COOLDOWN_SECONDS = int(os.getenv("DD_POST_COOLDOWN_SECONDS", "120") or "120")
    POST_RETRY_FAILED = os.getenv("DD_POST_RETRY_FAILED", "1") == "1"
    POST_MAX_ATTEMPTS = int(os.getenv("DD_POST_MAX_ATTEMPTS", "3") or "3")
    POPULATE_IMG_LINKS = os.getenv("DD_POPULATE_IMG_LINKS", "0") == "1"
    POPULATE_IMG_LINKS_WRITE = os.getenv("DD_POPULATE_IMG_LINKS_WRITE", "0") == "1"

    REKHTA_LISTING_URL = os.getenv("DD_REKHTA_LISTING_URL", "https://www.rekhta.org/shayari-image")
    REKHTA_MAX_SCROLLS = int(os.getenv("DD_REKHTA_MAX_SCROLLS", "6") or "6")
    REKHTA_POPULATE_WRITE = os.getenv("DD_REKHTA_POPULATE_WRITE", "0") == "1"
    REKHTA_POPULATE_LIMIT = int(os.getenv("DD_REKHTA_POPULATE_LIMIT", "0") or "0")

    POST_DENIED_RETRIES = int(os.getenv("DD_POST_DENIED_RETRIES", "1") or "1")
    POST_DENIED_BACKOFF_SECONDS = int(os.getenv("DD_POST_DENIED_BACKOFF_SECONDS", "600") or "600")

    POST_PRE_SUBMIT_DELAY_SECONDS = float(os.getenv("DD_POST_PRE_SUBMIT_DELAY_SECONDS", "2") or "2")
    POST_PRE_SUBMIT_JITTER_SECONDS = float(os.getenv("DD_POST_PRE_SUBMIT_JITTER_SECONDS", "2") or "2")
    POST_DENIED_BACKOFF_MULTIPLIER = float(os.getenv("DD_POST_DENIED_BACKOFF_MULTIPLIER", "2") or "2")
    POST_DENIED_BACKOFF_JITTER_SECONDS = float(os.getenv("DD_POST_DENIED_BACKOFF_JITTER_SECONDS", "10") or "10")
    POST_MAX_CONSECUTIVE_DENIED = int(os.getenv("DD_POST_MAX_CONSECUTIVE_DENIED", "3") or "3")

    POST_MAX_REPEAT_CHARS = int(os.getenv("DD_POST_MAX_REPEAT_CHARS", "6") or "6")
    POST_CAPTION_MAX_LEN = int(os.getenv("DD_POST_CAPTION_MAX_LEN", "300") or "300")
    POST_TAGS_MAX_LEN = int(os.getenv("DD_POST_TAGS_MAX_LEN", "120") or "120")

    IMAGE_DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("DD_IMAGE_DOWNLOAD_TIMEOUT_SECONDS", "90") or "90")
    IMAGE_DOWNLOAD_RETRIES = int(os.getenv("DD_IMAGE_DOWNLOAD_RETRIES", "3") or "3")
    IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS = int(os.getenv("DD_IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS", "5") or "5")

    # URLs
    BASE_URL = "https://damadam.pk"
    LOGIN_URL = f"{BASE_URL}/login/"
    HOME_URL = BASE_URL

    # Logging
    LOG_DIR = Path("logs")
    LOG_DIR.mkdir(exist_ok=True)

# ============================================================================
# LOGGER
# ============================================================================

class Logger:
    """Enhanced logger with file and console output"""

    def __init__(self, mode: str = "general"):
        self.mode = mode
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = Config.LOG_DIR / f"{mode}_{timestamp}.log"

        # Create log file
        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write(f"DamaDam Bot - {mode.upper()} Mode\n")
            f.write(f"Started: {datetime.now()}\n")
            f.write("=" * 70 + "\n\n")

    def _log(self, message: str, level: str = "INFO"):
        """Internal log method"""
        pkt_time = self._get_pkt_time()
        timestamp = pkt_time.strftime("%H:%M:%S")
        safe_message = message if isinstance(message, str) else str(message)

        # Console output with colors
        color_map = {
            "INFO": "white",
            "SUCCESS": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "DEBUG": "cyan"
        }
        color = color_map.get(level, "white")

        try:
            if level == "INFO":
                console.print(f"[{timestamp}] {safe_message}")
            else:
                console.print(f"[{timestamp}] [{level}] {safe_message}", style=color)
        except UnicodeEncodeError:
            safe_ascii = self._sanitize_message(safe_message)
            if level == "INFO":
                console.print(f"[{timestamp}] {safe_ascii}")
            else:
                console.print(f"[{timestamp}] [{level}] {safe_ascii}", style=color)

        # File output
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] [{level}] {safe_message}\n")

    def info(self, msg: str):
        self._log(msg, "INFO")

    def success(self, msg: str):
        self._log(msg, "SUCCESS")

    def warning(self, msg: str):
        self._log(msg, "WARNING")

    def error(self, msg: str):
        self._log(msg, "ERROR")

    def debug(self, msg: str):
        if Config.DEBUG:
            self._log(msg, "DEBUG")

    @staticmethod
    def _get_pkt_time():
        """Get Pakistan time"""
        return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

    @staticmethod
    def _sanitize_message(message: str) -> str:
        """Strip non-ASCII characters for Windows console compatibility."""
        if not isinstance(message, str):
            message = str(message)
        return message.encode("ascii", "ignore").decode("ascii")

# ============================================================================
# BROWSER MANAGER
# ============================================================================

class BrowserManager:
    """Manages browser setup and authentication"""

    def __init__(self, logger: Logger):
        self.logger = logger
        self.driver = None

    def setup(self):
        """Setup headless Chrome browser"""
        try:
            opts = Options()
            if Config.HEADLESS:
                opts.add_argument("--headless=new")
            opts.add_argument("--window-size=1920,1080")
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            opts.add_experimental_option("useAutomationExtension", False)
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--disable-logging")
            opts.add_argument("--log-level=3")
            opts.page_load_strategy = "eager"

            # Try to use specified chromedriver, fallback to Selenium Manager on mismatch
            driver_path = (Config.CHROMEDRIVER_PATH or "").strip()
            if driver_path.lower() in {"auto", "none", "false", "0"}:
                driver_path = ""

            if driver_path and not os.path.isabs(driver_path):
                driver_path = str(Path(__file__).resolve().parent / driver_path)

            if driver_path:
                if os.path.exists(driver_path):
                    try:
                        devnull = open(os.devnull, "w")
                        service = Service(driver_path, service_args=["--log-level=OFF"], log_output=devnull)
                    except Exception:
                        service = Service(driver_path)

                    try:
                        self.driver = webdriver.Chrome(service=service, options=opts)
                    except Exception as e:
                        self.logger.warning(
                            f"Local ChromeDriver failed ({str(e)[:200]}). Falling back to Selenium Manager."
                        )
                        self.driver = webdriver.Chrome(options=opts)
                else:
                    self.logger.warning(
                        f"ChromeDriver not found at '{driver_path}'. Falling back to Selenium Manager."
                    )
                    self.driver = webdriver.Chrome(options=opts)
            else:
                self.driver = webdriver.Chrome(options=opts)

            self.driver.set_page_load_timeout(45)
            self.driver.execute_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )

            self.logger.debug("Browser setup complete")
            return self.driver

        except Exception as e:
            self.logger.error(f"Browser setup failed: {e}")
            return None

    def login(self) -> bool:
        """Login to DamaDam"""
        if not self.driver:
            return False

        def attempt_login(user: str, pwd: str) -> bool:
            if not user or not pwd:
                return False
            try:
                self.driver.get(Config.LOGIN_URL)
                time.sleep(3)

                nick_input = WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#nick, input[name='nick']"))
                )
                pass_input = self.driver.find_element(By.CSS_SELECTOR, "#pass, input[name='pass']")
                submit_btn = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")

                nick_input.clear()
                nick_input.send_keys(user)
                time.sleep(0.5)

                pass_input.clear()
                pass_input.send_keys(pwd)
                time.sleep(0.5)

                submit_btn.click()
                time.sleep(4)

                if "login" not in self.driver.current_url.lower():
                    self._save_cookies()
                    return True
                return False
            except Exception:
                return False

        try:
            # Try loading cookies first
            if self._load_cookies():
                self.driver.get(Config.HOME_URL)
                time.sleep(2)

                # Check if still logged in
                current_url = self.driver.current_url.lower()
                if "login" not in current_url and "signup" not in current_url:
                    self.logger.debug("Logged in via cookies")
                    return True
                else:
                    self.logger.debug("Cookies expired, fresh login needed")

            # Fresh login (primary -> secondary fallback)
            self.logger.debug("Performing fresh login...")
            if attempt_login(Config.LOGIN_EMAIL, Config.LOGIN_PASS):
                self.logger.debug("Fresh login successful")
                return True

            if Config.LOGIN_EMAIL2 and Config.LOGIN_PASS2:
                self.logger.warning("Primary login failed, trying secondary credentials...")
                if attempt_login(Config.LOGIN_EMAIL2, Config.LOGIN_PASS2):
                    self.logger.debug("Secondary login successful")
                    return True

            self.logger.error("Login failed - check credentials")
            return False

        except Exception as e:
            self.logger.error(f"Login error: {e}")
            return False

    def _save_cookies(self):
        """Save cookies to file"""
        try:
            with open(Config.COOKIE_FILE, "wb") as f:
                pickle.dump(self.driver.get_cookies(), f)
            self.logger.debug("Cookies saved")
        except Exception as e:
            self.logger.warning(f"Cookie save failed: {e}")

    def _load_cookies(self) -> bool:
        """Load cookies from file"""
        try:
            if not os.path.exists(Config.COOKIE_FILE):
                return False

            self.driver.get(Config.HOME_URL)
            time.sleep(2)

            with open(Config.COOKIE_FILE, "rb") as f:
                cookies = pickle.load(f)

            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception:
                    pass

            self.driver.refresh()
            time.sleep(2)
            self.logger.debug("Cookies loaded")
            return True

        except Exception as e:
            self.logger.debug(f"Cookie load failed: {e}")
            return False

    def close(self):
        """Close browser"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.debug("Browser closed")
            except Exception:
                pass

# ============================================================================
# SHEETS MANAGER
# ============================================================================

class SheetsManager:
    """Manages Google Sheets operations with retry logic"""

    def __init__(self, logger: Logger):
        self.logger = logger
        self.client = None
        self.api_calls = 0

    def connect(self) -> bool:
        """Connect to Google Sheets"""
        try:
            if not os.path.exists(Config.CREDENTIALS_FILE):
                self.logger.error(f"{Config.CREDENTIALS_FILE} not found")
                return False

            scope = ["https://www.googleapis.com/auth/spreadsheets"]
            creds = Credentials.from_service_account_file(
                Config.CREDENTIALS_FILE,
                scopes=scope
            )
            self.client = gspread.authorize(creds)
            self.logger.debug("Connected to Google Sheets API")
            return True

        except Exception as e:
            self.logger.error(f"Sheets connection failed: {e}")
            return False

    def get_sheet(self, sheet_id: str, sheet_name: str, create_if_missing: bool = True):
        """Get or create worksheet"""
        try:
            workbook = self.client.open_by_key(sheet_id)

            # Try to get existing sheet
            try:
                sheet = workbook.worksheet(sheet_name)
                self.logger.debug(f"Found sheet: {sheet_name}")
                return sheet
            except WorksheetNotFound:
                if Config.DRY_RUN:
                    self.logger.warning(f"[DRY RUN] Sheet '{sheet_name}' not found (not creating in dry-run)")
                    return None
                if not create_if_missing:
                    return None
                self.logger.warning(f"Sheet '{sheet_name}' not found, creating...")
                return self._create_sheet(workbook, sheet_name)

        except Exception as e:
            self.logger.error(f"Failed to get sheet '{sheet_name}': {e}")
            return None

    def _create_sheet(self, workbook, sheet_name: str):
        """Create new worksheet with appropriate headers"""
        if Config.DRY_RUN:
            self.logger.warning(f"[DRY RUN] Skipping sheet creation: {sheet_name}")
            return None

        # Define headers for each sheet type
        headers_map = {
            "MsgList": [
                "MODE", "NAME", "NICK/URL", "CITY", "POSTS", "FOLLOWERS", "Gender",
                "MESSAGE", "STATUS", "NOTES", "RESULT URL"
            ],
            "MsgQueue": [
                "TIMESTAMP", "NICK", "NAME", "MESSAGE", "POST_URL", "STATUS", "NOTES", "RESULT_URL"
            ],
            "PostQueue": [
                "STATUS", "TITLE", "TITLE_UR", "IMAGE_PATH", "TYPE",
                "POST_URL", "TIMESTAMP", "NOTES", "SIGNATURE"
            ],
            "PostQueueLog": [
                "TIMESTAMP", "TYPE", "TITLE", "IMAGE_PATH", "POST_URL", "STATUS", "NOTES"
            ],
            "InboxQueue": [
                "NICK", "NAME", "LAST_MSG", "MY_REPLY", "STATUS",
                "TIMESTAMP", "NOTES", "CONVERSATION_LOG"
            ],
            "Inbox": [
                "NICK", "NAME", "LAST_MSG", "MY_REPLY", "STATUS",
                "TIMESTAMP", "NOTES", "CONVERSATION_LOG"
            ],
            "Inbox & Activity": [
                "NICK", "NAME", "LAST_MSG", "MY_REPLY", "STATUS",
                "TIMESTAMP", "NOTES", "CONVERSATION_LOG"
            ],
            "MasterLog": [
                "TIMESTAMP", "MODE", "ACTION", "NICK", "URL", "STATUS", "DETAILS"
            ],
            # Legacy sheet names kept for backward compat
            "MsgHistory": [
                "TIMESTAMP", "NICK", "NAME", "MESSAGE", "POST_URL",
                "STATUS", "RESULT_URL"
            ],
            "PostHistory": [
                "TIMESTAMP", "TYPE", "TITLE", "IMAGE_PATH", "POST_URL",
                "STATUS", "NOTES"
            ],
            "Logs": [
                "TIMESTAMP", "MODE", "ACTION", "NICK", "URL", "STATUS", "DETAILS"
            ],
            "ConversationLog": [
                "TIMESTAMP", "NICK", "DIRECTION", "MODE", "MESSAGE", "URL", "STATUS"
            ],
        }

        headers = headers_map.get(sheet_name, ["DATA"])

        try:
            sheet = workbook.add_worksheet(
                title=sheet_name,
                rows=1000,
                cols=len(headers)
            )
            sheet.insert_row(headers, 1)
            self._format_headers(sheet, len(headers))
            self.logger.success(f"Created sheet: {sheet_name}")
            return sheet
        except Exception as e:
            self.logger.error(f"Failed to create sheet '{sheet_name}': {e}")
            return None

    def ensure_postqueue_headers(self, sheet) -> bool:
        """Ensure PostQueue has required headers; append missing columns if needed."""
        required = [
            "STATUS", "TITLE", "TITLE_UR", "IMAGE_PATH", "TYPE",
            "POST_URL", "TIMESTAMP", "NOTES", "SIGNATURE"
        ]
        try:
            headers = sheet.row_values(1)
        except Exception as e:
            self.logger.error(f"Failed to read PostQueue headers: {e}")
            return False

        if not headers:
            try:
                if Config.DRY_RUN:
                    self.logger.warning("[DRY RUN] PostQueue headers missing; skipping insert")
                    return True
                sheet.insert_row(required, 1)
                self._format_headers(sheet, len(required))
                self.logger.success("PostQueue headers inserted")
                return True
            except Exception as e:
                self.logger.error(f"Failed to insert PostQueue headers: {e}")
                return False

        upper_headers = [(h or "").strip().upper() for h in headers]
        missing = [h for h in required if h not in upper_headers]
        if not missing:
            return True

        new_headers = headers + missing
        try:
            if Config.DRY_RUN:
                self.logger.warning(f"[DRY RUN] PostQueue missing headers ({', '.join(missing)}); skipping update")
                return True
            end_cell = rowcol_to_a1(1, len(new_headers))
            sheet.update(values=[new_headers], range_name=f"A1:{end_cell}")
            self._format_headers(sheet, len(new_headers))
            self.logger.success(f"PostQueue headers updated (added: {', '.join(missing)})")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update PostQueue headers: {e}")
            return False

    def _format_headers(self, sheet, col_count: int):
        """Freeze header row and apply basic formatting."""
        if Config.DRY_RUN:
            return
        try:
            sheet.freeze(rows=1)
            header_range = f"A1:{rowcol_to_a1(1, col_count)}"
            sheet.format(
                header_range,
                {
                    "textFormat": {
                        "bold": True,
                        "fontFamily": "Quantico",
                        "foregroundColor": {"red": 0.98, "green": 0.98, "blue": 0.98}
                    },
                    "horizontalAlignment": "CENTER",
                    "backgroundColor": {"red": 0.12, "green": 0.14, "blue": 0.18}
                }
            )
        except Exception as e:
            self.logger.debug(f"Header formatting failed: {e}")

    def _apply_row_banding(self, sheet, col_count: int, row_count: int = 1000):
        if Config.DRY_RUN:
            return
        try:
            req = {
                "addBanding": {
                    "bandedRange": {
                        "range": {
                            "sheetId": sheet.id,
                            "startRowIndex": 1,
                            "endRowIndex": row_count,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_count
                        },
                        "rowProperties": {
                            "firstBandColor": {"red": 0.98, "green": 0.99, "blue": 1.0},
                            "secondBandColor": {"red": 0.95, "green": 0.96, "blue": 0.98}
                        }
                    }
                }
            }
            sheet.spreadsheet.batch_update({"requests": [req]})
        except Exception as e:
            self.logger.debug(f"Row banding failed: {e}")

    def _apply_dropdowns(self, sheet, header_map: Dict[str, int]):
        if Config.DRY_RUN:
            return
        try:
            requests = []

            def _add_dropdown(col_idx: int, values: List[str]):
                if col_idx < 1:
                    return
                requests.append({
                    "setDataValidation": {
                        "range": {
                            "sheetId": sheet.id,
                            "startRowIndex": 1,
                            "endRowIndex": 1000,
                            "startColumnIndex": col_idx - 1,
                            "endColumnIndex": col_idx
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [{"userEnteredValue": v} for v in values]
                            },
                            "showCustomUi": True,
                            "strict": True
                        }
                    }
                })

            def _add_color_rule(col_idx: int, text_value: str, color: Dict[str, float]):
                requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{
                                "sheetId": sheet.id,
                                "startRowIndex": 1,
                                "endRowIndex": 1000,
                                "startColumnIndex": col_idx - 1,
                                "endColumnIndex": col_idx
                            }],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_EQ",
                                    "values": [{"userEnteredValue": text_value}]
                                },
                                "format": {
                                    "backgroundColor": color,
                                    "textFormat": {"bold": True}
                                }
                            }
                        },
                        "index": 0
                    }
                })

            def _find(*keys: str) -> int:
                for k in keys:
                    kk = (k or "").strip().upper()
                    if kk in header_map:
                        return header_map[kk] + 1
                return 0

            status_col = _find("STATUS", "STATU")
            type_col = _find("TYPE")

            if status_col:
                status_values = ["Pending", "Done", "Failed", "Skipped", "Repeating", "Sent"]
                _add_dropdown(status_col, status_values)
                _add_color_rule(status_col, "Pending", {"red": 0.99, "green": 0.91, "blue": 0.7})
                _add_color_rule(status_col, "Done", {"red": 0.8, "green": 0.95, "blue": 0.8})
                _add_color_rule(status_col, "Failed", {"red": 0.98, "green": 0.8, "blue": 0.8})
                _add_color_rule(status_col, "Skipped", {"red": 0.9, "green": 0.9, "blue": 0.9})
                _add_color_rule(status_col, "Repeating", {"red": 1.0, "green": 0.9, "blue": 0.75})
                _add_color_rule(status_col, "Sent", {"red": 0.82, "green": 0.9, "blue": 1.0})

            if type_col:
                type_values = ["text", "image"]
                _add_dropdown(type_col, type_values)
                _add_color_rule(type_col, "text", {"red": 0.88, "green": 0.92, "blue": 1.0})
                _add_color_rule(type_col, "image", {"red": 0.95, "green": 0.9, "blue": 0.8})

            if requests:
                sheet.spreadsheet.batch_update({"requests": requests})
        except Exception as e:
            self.logger.debug(f"Dropdown styling failed: {e}")

    def style_sheet(self, sheet) -> None:
        if Config.DRY_RUN or not sheet:
            return
        try:
            headers = sheet.row_values(1)
        except Exception:
            headers = []
        col_count = len(headers) if headers else 1
        header_map: Dict[str, int] = {}
        for idx, h in enumerate(headers):
            key = (h or "").strip().upper()
            if key and key not in header_map:
                header_map[key] = idx
        self._format_headers(sheet, col_count)
        self._apply_row_banding(sheet, col_count)
        self._apply_dropdowns(sheet, header_map)

    def update_cell(self, sheet, row: int, col: int, value, retries: int = 3):
        """Update cell with retry logic"""
        if Config.DRY_RUN:
            self.logger.debug(f"[DRY RUN] update_cell({row},{col})={str(value)[:120]}")
            return True
        for attempt in range(retries):
            try:
                self.api_calls += 1
                sheet.update_cell(row, col, value)
                return True
            except Exception as e:
                if attempt == retries - 1:
                    self.logger.error(f"Cell update failed ({row},{col}): {e}")
                    return False
                self.logger.debug(f"Retry {attempt+1}/{retries} for cell ({row},{col})")
                time.sleep(2 ** attempt)
        return False

    def delete_rows(self, sheet, start: int, end: Optional[int] = None):
        if Config.DRY_RUN:
            self.logger.debug(f"[DRY RUN] delete_rows({start}, {end})")
            return True
        try:
            if end is None or end <= start:
                sheet.delete_rows(start)
            else:
                sheet.delete_rows(start, end)
            return True
        except Exception as e:
            self.logger.warning(f"Delete rows failed: {e}")
            return False

    def append_row(self, sheet, values: list, retries: int = 3):
        """Append row with retry logic"""
        if Config.DRY_RUN:
            self.logger.debug(f"[DRY RUN] append_row(len={len(values)})")
            return True
        for attempt in range(retries):
            try:
                self.api_calls += 1
                sheet.append_row(values)
                return True
            except Exception as e:
                if attempt == retries - 1:
                    self.logger.error(f"Row append failed: {e}")
                    return False
                self.logger.debug(f"Retry {attempt+1}/{retries} for append")
                time.sleep(2 ** attempt)
        return False

# ============================================================================
# PROFILE SCRAPER
# ============================================================================

class ProfileScraper:
    """Handles profile scraping and post finding"""

    def __init__(self, driver, logger: Logger):
        self.driver = driver
        self.logger = logger

    @staticmethod
    def _strip_non_bmp(text: str) -> str:
        if not text:
            return ""
        try:
            return "".join(ch for ch in text if ord(ch) <= 0xFFFF)
        except Exception:
            return text

    @staticmethod
    def _parse_rate_limit_seconds(text: str) -> int:
        t = (text or "").lower()
        m = re.search(r"(\d+)\s*(?:min|mins|minute|minutes)", t)
        if m:
            try:
                return max(30, int(m.group(1)) * 60)
            except Exception:
                return 120
        return 120

    def _detect_rate_limit(self) -> int:
        try:
            src = (self.driver.page_source or "")
        except Exception:
            src = ""
        low = src.lower()
        if "min baad" in low or "image share" in low or "2 min" in low:
            return self._parse_rate_limit_seconds(low)
        return 0

    def _detect_repeating_image(self) -> bool:
        try:
            src = (self.driver.page_source or "")
        except Exception:
            src = ""
        low = src.lower()
        # Exact matches from the actual duplicate-image page
        if "<title>duplicate image | damadam</title>" in low:
            return True
        if "duplicate image!" in low:
            return True
        if "is jesa image pehle upload ho chuka hai" in low:
            return True
        # Fallback generic keywords (kept for robustness)
        if "repeat" in low or "repeating" in low or "already" in low:
            return True
        if "same image" in low or "duplicate" in low or "pehle" in low:
            return True
        if "tasveer" in low and ("dobara" in low or "bar bar" in low or "phir" in low):
            return True
        return False

    def scrape_profile(self, nickname: str) -> Optional[Dict]:
        """Scrape user profile data"""
        safe_nick = quote(str(nickname).strip(), safe="+")
        url = f"{Config.BASE_URL}/users/{safe_nick}/"

        try:
            self.logger.debug(f"Scraping: {nickname}")
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.cxl, h1"))
            )

            # Initialize profile data
            data = {
                "NICK": nickname,
                "NAME": nickname,
                "CITY": "",
                "GENDER": "",
                "POSTS": "0",
                "FOLLOWERS": "0",
                "STATUS": "Unknown",
                "PROFILE_URL": url
            }

            page_source = self.driver.page_source.lower()

            # Check account status
            if "account suspended" in page_source:
                data["STATUS"] = "Suspended"
                self.logger.warning(f"Account suspended: {nickname}")
                return data
            elif "background:tomato" in page_source or "style=\"background:tomato\"" in page_source:
                data["STATUS"] = "Unverified"
            else:
                data["STATUS"] = "Verified"

            # Extract profile fields
            fields_map = {
                "City:": "CITY",
                "Gender:": "GENDER",
            }

            for label, key in fields_map.items():
                try:
                    elem = self.driver.find_element(
                        By.XPATH,
                        f"//b[contains(text(), '{label}')]/following-sibling::span[1]"
                    )
                    value = elem.text.strip()

                    if key == "GENDER":
                        low = value.lower()
                        data[key] = "🚺" if low == "female" else "🚹" if low == "male" else value
                    else:
                        data[key] = value
                except Exception:
                    continue

            # Extract posts count
            try:
                posts_elem = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "a[href*='/profile/public/'] button div:first-child"
                )
                match = re.search(r"(\d+)", posts_elem.text)
                if match:
                    data["POSTS"] = match.group(1)
            except Exception:
                pass

            # Extract followers count
            try:
                followers_elem = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "span.cl.sp.clb"
                )
                match = re.search(r"(\d+)", followers_elem.text)
                if match:
                    data["FOLLOWERS"] = match.group(1)
            except Exception:
                pass

            self.logger.debug(
                f"Profile: {data['GENDER']}, {data['CITY']}, "
                f"Posts: {data['POSTS']}, Status: {data['STATUS']}"
            )

            return data

        except TimeoutException:
            self.logger.error(f"Timeout scraping {nickname}")
            return None
        except Exception as e:
            self.logger.error(f"Scrape error for {nickname}: {e}")
            return None

    def find_open_post(self, nickname: str, post_type: str = "any") -> Optional[str]:
        """
        Find first open post (text or image)

        Args:
            nickname: User nickname
            post_type: 'text', 'image', or 'any'

        Returns:
            Post URL or None
        """
        safe_nick = quote(str(nickname).strip(), safe="+")
        url = f"{Config.BASE_URL}/profile/public/{safe_nick}/"

        try:
            self.logger.debug(f"Finding open post for: {nickname}")

            max_pages = Config.MAX_POST_PAGES if Config.MAX_POST_PAGES > 0 else 4

            for page_num in range(1, max_pages + 1):
                self.driver.get(url)
                time.sleep(3)

                # Scroll to load dynamic content
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # Find all posts on page
                posts = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    "article.mbl, article, div[class*='post'], div[class*='content']"
                )
                self.logger.debug(f"Page {page_num}: Found {len(posts)} posts")

                next_href = ""
                try:
                    next_link = self.driver.find_element(By.CSS_SELECTOR, "a[rel='next']")
                    next_href = next_link.get_attribute("href") or ""
                except Exception:
                    next_href = ""

                for idx, post in enumerate(posts, 1):
                    try:
                        # Look for comment links (both text and image)
                        selectors = []

                        if post_type in ["text", "any"]:
                            selectors.append("a[href*='/comments/text/']")
                        if post_type in ["image", "any"]:
                            selectors.append("a[href*='/comments/image/']")

                        href = ""
                        found_type = ""

                        for sel in selectors:
                            try:
                                link = post.find_element(By.CSS_SELECTOR, sel)
                                href = link.get_attribute("href") or ""
                                if href:
                                    found_type = "text" if "/comments/text/" in href else "image"
                                    break
                            except Exception:
                                continue
                        # Fallback: try reply button
                        if not href:
                            try:
                                reply_btn = post.find_element(
                                    By.XPATH,
                                    ".//a[button[@itemprop='discussionUrl']]"
                                )
                                href = reply_btn.get_attribute("href") or ""
                            except Exception:
                                continue

                        if href:
                            clean_href = self.clean_url(href)
                            self.logger.debug(f"Found {found_type} post #{idx}: {clean_href}")
                            return clean_href

                    except Exception as e:
                        self.logger.debug(f"Post #{idx} check failed: {e}")
                        continue

                # Fallback: search for comment/content links globally
                fallback_selectors = []
                if post_type in ["text", "any"]:
                    fallback_selectors.append("a[href*='/comments/text/']")
                if post_type in ["image", "any"]:
                    fallback_selectors.append("a[href*='/comments/image/']")
                fallback_selectors.append("a[href*='/content/']")

                for sel in fallback_selectors:
                    try:
                        links = self.driver.find_elements(By.CSS_SELECTOR, sel)
                        for link in links:
                            href = link.get_attribute("href") or ""
                            if href:
                                clean_href = self.clean_url(href)
                                self.logger.debug(f"Fallback found post: {clean_href}")
                                return clean_href
                    except Exception:
                        continue

                # JS fallback to collect all matching hrefs
                try:
                    hrefs = self.driver.execute_script(
                        "return Array.from(document.querySelectorAll(\"a[href*='/comments/'], a[href*='/content/']\"))"
                        ".map(a => a.href).filter(Boolean);"
                    )
                    for href in hrefs:
                        clean_href = self.clean_url(href)
                        if clean_href:
                            self.logger.debug(f"JS fallback found post: {clean_href}")
                            return clean_href
                except Exception:
                    pass

                # ID fallback: some profiles don't expose /comments/ links on profile page
                candidate_ids: List[str] = []
                try:
                    for post in posts[:30]:
                        outer = self.driver.execute_script("return arguments[0].outerHTML", post)
                        nums = re.findall(r"\b\d{7,10}\b", outer)
                        for n in nums:
                            try:
                                iv = int(n)
                            except Exception:
                                continue
                            if iv >= 1_000_000_000:
                                continue
                            if iv < 1_000_000:
                                continue

                            # Heuristic: prefer likely post IDs (usually 8-9 digits) over user IDs (often 7 digits)
                            if len(n) < 8:
                                continue
                            if n not in candidate_ids:
                                candidate_ids.append(n)
                except Exception:
                    candidate_ids = []

                if candidate_ids:
                    kinds: List[str]
                    if post_type == "text":
                        kinds = ["text"]
                    elif post_type == "image":
                        kinds = ["image"]
                    else:
                        kinds = ["text", "image"]

                    for pid in candidate_ids[:20]:
                        for kind in kinds:
                            try:
                                cand_url = f"{Config.BASE_URL}/comments/{kind}/{pid}"
                                self.driver.get(cand_url)
                                time.sleep(2)
                                src = self.driver.page_source.lower()
                                if "404" in src or "page not found" in src:
                                    continue

                                forms = self.driver.find_elements(
                                    By.CSS_SELECTOR,
                                    "form[action*='direct-response/send']"
                                )
                                if forms:
                                    # Don't rely on is_displayed() in headless; just validate the textarea exists.
                                    for f in forms:
                                        try:
                                            f.find_element(By.CSS_SELECTOR, "textarea[name='direct_response']")
                                            self.logger.debug(f"ID fallback found {kind} post: {cand_url}")
                                            return self.clean_url(self.driver.current_url)
                                        except Exception:
                                            continue

                            except Exception:
                                continue

                # Try next page
                if not next_href:
                    break
                url = next_href

            self.logger.warning(f"No open posts found for {nickname}")
            return None

        except Exception as e:
            self.logger.error(f"Error finding posts: {e}")
            return None

    @staticmethod
    def clean_url(url: str) -> str:
        """Clean and normalize post URLs"""
        if not url:
            return ""

        content_match = re.search(r"/content/(\d+)", url)
        if content_match:
            return f"{Config.BASE_URL}/comments/image/{content_match.group(1)}"

        url = str(url).strip()

        # Extract clean post ID
        text_match = re.search(r"/comments/text/(\d+)", url)
        if text_match:
            return f"{Config.BASE_URL}/comments/text/{text_match.group(1)}"

        image_match = re.search(r"/comments/image/(\d+)", url)
        if image_match:
            return f"{Config.BASE_URL}/comments/image/{image_match.group(1)}"

        # Remove reply fragments
        url = re.sub(r"/\d+/#reply$", "", url)
        url = re.sub(r"/#reply$", "", url)
        url = url.rstrip("/")

        return url

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if URL is valid DamaDam post URL"""
        if not url:
            return False
        return (
            "damadam.pk" in url
            and ("/comments/text/" in url or "/comments/image/" in url or "/content/" in url)
        )

# ============================================================================
# MESSAGE RECORDER
# ============================================================================

class MessageRecorder:
    """Records message history by nickname"""

    def __init__(self, sheets_manager: SheetsManager, logger: Logger):
        self.sheets = sheets_manager
        self.logger = logger
        self.history_sheet = None

    def initialize(self) -> bool:
        """Initialize MsgQueue sheet (replaces MsgHistory)"""
        # Try new name first, fall back to legacy
        self.history_sheet = self.sheets.get_sheet(Config.SHEET_ID, "MsgQueue")
        if not self.history_sheet:
            self.history_sheet = self.sheets.get_sheet(Config.SHEET_ID, "MsgHistory")
        if self.history_sheet:
            self.logger.debug("Message history tracking enabled (MsgQueue)")
            return True
        return False

    def record_message(self, nick: str, name: str, message: str,
                       post_url: str, status: str, result_url: str = ""):
        """Record a sent message"""
        if not self.history_sheet:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [timestamp, nick, name, message, post_url, status, result_url]

        self.sheets.append_row(self.history_sheet, values)
        self.logger.debug(f"Recorded message history for: {nick}")


class PostHistoryRecorder:
    """Records post history into PostHistory sheet"""

    def __init__(self, sheets_manager: SheetsManager, logger: Logger):
        self.sheets = sheets_manager
        self.logger = logger
        self.history_sheet = None

    def initialize(self) -> bool:
        # Try new name first, fall back to legacy
        self.history_sheet = self.sheets.get_sheet(Config.SHEET_ID, "PostQueueLog")
        if not self.history_sheet:
            self.history_sheet = self.sheets.get_sheet(Config.SHEET_ID, "PostHistory")
        if self.history_sheet:
            self.logger.debug("Post history tracking enabled (PostQueueLog)")
            return True
        return False

    def record_post(self, post_type: str, title: str, image_path: str,
                    post_url: str, status: str, notes: str = ""):
        if not self.history_sheet:
            return
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [timestamp, post_type, title, image_path, post_url, status, notes]
        self.sheets.append_row(self.history_sheet, values)

class PostQueueIndex:
    def __init__(self):
        self.keys: set = set()

    @staticmethod
    def _norm(s: str) -> str:
        return (s or "").strip().lower()

    def add(self, value: str) -> None:
        k = self._norm(value)
        if k:
            self.keys.add(k)

    def contains(self, value: str) -> bool:
        k = self._norm(value)
        return bool(k and k in self.keys)

    @classmethod
    def from_postqueue_values(cls, all_rows: List[List[str]], header_map: Dict[str, int]):
        idx = cls()
        if not all_rows:
            return idx

        headers = all_rows[0] if all_rows else []
        if not headers:
            return idx

        def find_col(*names: str) -> Optional[int]:
            for name in names:
                key = (name or "").strip().upper()
                if key in header_map:
                    return header_map[key]
            return None

        col_image_path = find_col("IMAGE_PATH", "IMG_LINK", "IMG", "IMAGE")
        col_source_url = find_col("SOURCE_URL", "POST_LINK", "POST_URL_SOURCE", "SOURCE")
        col_post_url = find_col("POST_URL", "RESULT_URL", "RESULT URL")

        for row in all_rows[1:]:
            try:
                if col_image_path is not None and len(row) > col_image_path:
                    idx.add(row[col_image_path])
                if col_source_url is not None and len(row) > col_source_url:
                    idx.add(row[col_source_url])
                if col_post_url is not None and len(row) > col_post_url:
                    idx.add(row[col_post_url])
            except Exception:
                continue
        return idx

class ActivityLogger:
    def __init__(self, sheets_manager: SheetsManager, logger: Logger):
        self.sheets = sheets_manager
        self.logger = logger
        self.sheet = None

    def initialize(self) -> bool:
        # Try new MasterLog first, fall back to legacy Logs
        self.sheet = self.sheets.get_sheet(Config.SHEET_ID, "MasterLog")
        if not self.sheet:
            self.sheet = self.sheets.get_sheet(Config.SHEET_ID, "Logs")
        return bool(self.sheet)

    def log(self, mode: str, action: str, nick: str = "", url: str = "", status: str = "", details: str = ""):
        if not self.sheet:
            return
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [timestamp, mode, action, nick, url, status, (details or "")[:45000]]
        self.sheets.append_row(self.sheet, values)


class ConversationLogger:
    def __init__(self, sheets_manager: SheetsManager, logger: Logger):
        self.sheets = sheets_manager
        self.logger = logger
        self.sheet = None

    def initialize(self) -> bool:
        # Try new MasterLog first, fall back to legacy ConversationLog
        self.sheet = self.sheets.get_sheet(Config.SHEET_ID, "MasterLog")
        if not self.sheet:
            self.sheet = self.sheets.get_sheet(Config.SHEET_ID, "ConversationLog")
        return bool(self.sheet)

    def log(self, nick: str, direction: str, mode: str, message: str, url: str = "", status: str = ""):
        if not self.sheet:
            return
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [timestamp, nick, direction, mode, (message or "")[:45000], url, status]
        self.sheets.append_row(self.sheet, values)

# ============================================================================
# MESSAGE SENDER
# ============================================================================

class MessageSender:
    """Handles sending messages to posts"""

    def __init__(self, driver, logger: Logger, scraper: ProfileScraper, recorder: MessageRecorder):
        self.driver = driver
        self.logger = logger
        self.scraper = scraper
        self.recorder = recorder

    @staticmethod
    def _strip_non_bmp(text: str) -> str:
        if not text:
            return ""
        try:
            return "".join(ch for ch in text if ord(ch) <= 0xFFFF)
        except Exception:
            return text

    def send_message(self, post_url: str, message: str, nick: str = "") -> Dict:
        """Send message to a post and verify"""
        if Config.DRY_RUN:
            self.logger.info(f"[DRY RUN] Would send message to {post_url} (nick={nick})")
            return {"status": "Dry Run", "url": post_url}
        try:
            self.logger.debug(f"Opening post: {post_url}")
            self.driver.get(post_url)
            time.sleep(3)

            page_source = self.driver.page_source

            # Check for blocks
            if "FOLLOW TO REPLY" in page_source.upper():
                self.logger.warning("Must follow user first")
                return {"status": "Not Following", "url": post_url}

            if "comments are closed" in page_source.lower() or "comments closed" in page_source.lower():
                self.logger.warning("Comments closed")
                return {"status": "Comments Closed", "url": post_url}

            # Find visible comment form
            forms = self.driver.find_elements(
                By.CSS_SELECTOR,
                "form[action*='direct-response/send']"
            )

            form = None
            for f in forms:
                if f.is_displayed():
                    try:
                        f.find_element(By.CSS_SELECTOR, "textarea[name='direct_response']")
                        form = f
                        break
                    except Exception:
                        continue

            if not form:
                self.logger.warning("No visible comment form found")
                return {"status": "No Form", "url": post_url}

            # Find textarea and submit button
            textarea = form.find_element(
                By.CSS_SELECTOR,
                "textarea[name='direct_response']"
            )
            send_btn = form.find_element(
                By.CSS_SELECTOR,
                "button[type='submit']"
            )

            safe_message = self._strip_non_bmp(message)
            if safe_message != message:
                self.logger.debug("Message contained non-BMP characters; stripping for ChromeDriver compatibility")
            message = safe_message

            # Limit message to 350 chars
            if len(message) > 350:
                message = message[:350]
                self.logger.debug("Message truncated to 350 chars")

            # Type message
            textarea.clear()
            time.sleep(0.5)
            textarea.send_keys(message)
            self.logger.debug(f"Message entered: {len(message)} chars")
            time.sleep(1)

            # Submit
            self.logger.debug("Submitting message...")
            self.driver.execute_script("arguments[0].click();", send_btn)
            time.sleep(3)

            # Verify by refreshing and checking
            self.logger.debug("Verifying message...")
            self.driver.get(post_url)
            time.sleep(2)

            fresh_page = self.driver.page_source

            # Check multiple verification methods
            verifications = {
                "username": Config.LOGIN_EMAIL in fresh_page,
                "message": message in fresh_page,
                "recent": any(x in fresh_page.lower() for x in ["sec ago", "secs ago", "just now"])
            }

            if Config.DEBUG:
                for check, result in verifications.items():
                    self.logger.debug(f"Verify {check}: {'✓' if result else '✗'}")

            if verifications["username"] and verifications["message"]:
                self.logger.success("Message verified!")

                # Record to history
                if nick:
                    self.recorder.record_message(
                        nick=nick,
                        name=nick,
                        message=message,
                        post_url=post_url,
                        status="Posted",
                        result_url=post_url
                    )

                return {"status": "Posted", "url": post_url}
            else:
                self.logger.warning("Message sent but not verified")
                return {"status": "Pending Verification", "url": post_url}

        except NoSuchElementException as e:
            self.logger.error(f"Form element not found: {e}")
            return {"status": "Form Error", "url": post_url}
        except Exception as e:
            self.logger.error(f"Send error: {e}")
            return {"status": f"Error: {str(e)[:50]}", "url": post_url}

    def process_template(self, template: str, profile: Dict) -> str:
        """Process message template with profile data"""
        message = template

        replacements = {
            "{{name}}": (profile.get("NAME") or ""),
            "{{nick}}": (profile.get("NICK") or ""),
            "{{city}}": (profile.get("CITY") or ""),
            "{{posts}}": str(profile.get("POSTS") or ""),
            "{{followers}}": str(profile.get("FOLLOWERS") or ""),
            "{{gender}}": (profile.get("GENDER") or profile.get("Gender") or ""),
        }

        for placeholder, value in replacements.items():
            message = message.replace(placeholder, value)

        if not replacements.get("{{city}}", "").strip():
            message = re.sub(r"(?i)(?:,\s*)?no\s*city\b", "", message)

        message = re.sub(r"\{\{[^}]+\}\}", "", message)
        message = re.sub(r"\s+", " ", message).strip()
        message = re.sub(r"\s+([,?.!])", r"\1", message)
        message = re.sub(r",\s*,", ",", message)
        return message.strip()

# ============================================================================
# POST CREATOR
# ============================================================================

class PostCreator:
    """Handles creating new posts (text/image)"""

    def __init__(self, driver, logger: Logger):
        self.driver = driver
        self.logger = logger

    @staticmethod
    def _strip_non_bmp(text: str) -> str:
        if not text:
            return ""
        try:
            return "".join(ch for ch in text if ord(ch) <= 0xFFFF)
        except Exception:
            return text

    @staticmethod
    def _parse_rate_limit_seconds(text: str) -> int:
        t = (text or "").lower()
        m = re.search(r"(\d+)\s*(?:min|mins|minute|minutes)", t)
        if m:
            try:
                return max(30, int(m.group(1)) * 60)
            except Exception:
                return 120
        return 120

    def _detect_rate_limit(self) -> int:
        try:
            src = (self.driver.page_source or "")
        except Exception:
            src = ""
        low = src.lower()
        if "min baad" in low or "image share" in low or "2 min" in low:
            return self._parse_rate_limit_seconds(low)
        return 0

    def _detect_repeating_image(self) -> bool:
        try:
            src = (self.driver.page_source or "")
        except Exception:
            src = ""
        low = src.lower()
        if "<title>duplicate image | damadam</title>" in low:
            return True
        if "duplicate image!" in low:
            return True
        if "is jesa image pehle upload ho chuka hai" in low:
            return True
        if "repeat" in low or "repeating" in low or "already" in low:
            return True
        if "same image" in low or "duplicate" in low or "pehle" in low:
            return True
        if "tasveer" in low and ("dobara" in low or "bar bar" in low or "phir" in low):
            return True
        return False

    def _find_share_form(self, require_file: bool) -> Optional[object]:
        forms = self.driver.find_elements(By.CSS_SELECTOR, "form")
        for f in forms:
            try:
                has_submit = bool(
                    f.find_elements(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
                )
                if not has_submit:
                    continue

                has_file = bool(f.find_elements(By.CSS_SELECTOR, "input[type='file']"))
                if require_file and not has_file:
                    continue

                has_textarea = bool(f.find_elements(By.CSS_SELECTOR, "textarea"))
                if not has_textarea and not has_file:
                    continue

                return f
            except Exception:
                continue
        return None

    def _extract_post_url(self) -> str:
        try:
            current = self.driver.current_url
            if "/comments/" in current or "/content/" in current:
                return ProfileScraper.clean_url(current)

            try:
                canonical = self.driver.find_elements(By.CSS_SELECTOR, "link[rel='canonical']")
                if canonical:
                    href = (canonical[0].get_attribute("href") or "").strip()
                    if href and ("/comments/" in href or "/content/" in href):
                        return ProfileScraper.clean_url(href)
            except Exception:
                pass

            try:
                og = self.driver.find_elements(By.CSS_SELECTOR, "meta[property='og:url']")
                if og:
                    href = (og[0].get_attribute("content") or "").strip()
                    if href and ("/comments/" in href or "/content/" in href):
                        return ProfileScraper.clean_url(href)
            except Exception:
                pass

            links = self.driver.find_elements(
                By.CSS_SELECTOR,
                "a[href*='/comments/text/'], a[href*='/comments/image/'], a[href*='/content/']"
            )
            for a in links:
                try:
                    href = (a.get_attribute("href") or "").strip()
                    if href and "damadam.pk" in href:
                        return ProfileScraper.clean_url(href)
                except Exception:
                    continue

            try:
                html = self.driver.page_source
                m = re.search(r"https?://[^\s\"']*(/comments/(?:text|image)/\d+|/content/\d+)", html)
                if m:
                    return ProfileScraper.clean_url(m.group(0))

                m2 = re.search(r"(/comments/(?:text|image)/\d+|/content/\d+)", html)
                if m2:
                    return ProfileScraper.clean_url(f"{Config.BASE_URL}{m2.group(1)}")
            except Exception:
                pass
        except Exception:
            pass
        return ProfileScraper.clean_url(self.driver.current_url)

    def create_text_post(self, title: str, content: str, tags: str = "") -> Dict:
        """Create a text post"""
        if Config.DRY_RUN:
            self.logger.info("[DRY RUN] Would create text post")
            return {"status": "Dry Run", "url": ""}
        try:
            self.logger.info("Creating text post...")
            self.driver.get(f"{Config.BASE_URL}/share/text/")
            time.sleep(3)

            form = self._find_share_form(require_file=False)
            if not form:
                self.logger.error("Text post form not found")
                return {"status": "Form Error", "url": ""}

            title_input = None
            try:
                title_input = form.find_element(
                    By.CSS_SELECTOR,
                    "input[name='title'], #id_title, input[name='heading'], input[name='subject']"
                )
            except Exception:
                title_input = None

            content_area = form.find_element(
                By.CSS_SELECTOR,
                "textarea[name='text'], #id_text, textarea[name='content'], #id_content, textarea"
            )

            submit_btn = form.find_element(
                By.CSS_SELECTOR,
                "button[type='submit'], input[type='submit'], button.btn-primary, button.btn"
            )

            if title_input and title:
                title = self._strip_non_bmp(title)
                try:
                    title_input.clear()
                except Exception:
                    pass
                title_input.send_keys(title)
                time.sleep(0.5)

            content = self._strip_non_bmp(content)
            try:
                content_area.clear()
            except Exception:
                pass
            content_area.send_keys(content)
            time.sleep(0.5)

            if tags:
                try:
                    tags = self._sanitize_tags(tags)
                    tags_input = form.find_element(By.CSS_SELECTOR, "input[name='tags'], #id_tags")
                    try:
                        tags_input.clear()
                    except Exception:
                        pass
                    tags_input.send_keys(tags)
                except Exception:
                    pass

            self._select_radio_option(form=form, name="exp", value="i", label_text="Never expire post")
            self._select_radio_option(form=form, name="com", value="0", label_text="Yes")

            try:
                exp_first = form.find_elements(By.CSS_SELECTOR, "#exp-first")
                if exp_first and not exp_first[0].is_selected():
                    self.driver.execute_script("arguments[0].click();", exp_first[0])
            except Exception:
                pass

            try:
                com_off = form.find_elements(By.CSS_SELECTOR, "#com-off")
                if com_off and not com_off[0].is_selected():
                    self.driver.execute_script("arguments[0].click();", com_off[0])
            except Exception:
                pass

            self.logger.info("Submitting text post...")
            self.driver.execute_script("arguments[0].click();", submit_btn)
            try:
                WebDriverWait(self.driver, 10).until(lambda d: d.current_url != f"{Config.BASE_URL}/share/text/")
            except TimeoutException:
                pass
            time.sleep(2)

            wait_s = self._detect_rate_limit()
            if wait_s:
                self.logger.warning(f"Rate limit detected. Wait ~{wait_s}s then retry.")
                return {"status": "Rate Limited", "url": ProfileScraper.clean_url(self.driver.current_url), "wait_seconds": wait_s}

            post_url = self._extract_post_url()
            if self._is_denied_or_share_url(post_url):
                self.logger.error(f"Text post denied (url={post_url})")
                return {"status": "Denied", "url": post_url}

            if "damadam.pk" in post_url and "/comments/text/" in post_url:
                self.logger.success(f"Text post created: {post_url}")
                return {"status": "Posted", "url": post_url}

            self.logger.warning(f"Post submitted but URL unclear (url={post_url})")
            return {"status": "Pending Verification", "url": post_url}

        except Exception as e:
            self.logger.error(f"Text post error: {e}")
            return {"status": f"Error: {str(e)[:50]}", "url": ""}

    def create_image_post(self, image_path: str, title: str = "", content: str = "", tags: str = "") -> Dict:
        """Create an image post from local file or remote URL (download to temp)."""
        if Config.DRY_RUN:
            self.logger.info("[DRY RUN] Would create image post")
            return {"status": "Dry Run", "url": ""}
        try:
            self.logger.info("Creating image post...")

            temp_file = ""
            is_temp = False
            try:
                resolved_path, is_temp = self._resolve_image_to_local_path(image_path)
                if is_temp:
                    temp_file = resolved_path
                image_path = resolved_path
            except Exception as e:
                self.logger.error(f"Image download failed: {e}")
                return {"status": "Image Download Failed", "url": ""}

            if not os.path.exists(image_path):
                self.logger.error(f"Image not found: {image_path}")
                return {"status": "File Not Found", "url": ""}

            self.driver.get(f"{Config.BASE_URL}/share/photo/upload/")
            time.sleep(3)

            form = self._find_share_form(require_file=True)
            if not form:
                self.logger.error("Image upload form not found")
                return {"status": "Form Error", "url": ""}

            file_input = form.find_element(
                By.CSS_SELECTOR,
                "input[type='file'], input[name='file'], input[name='image']"
            )

            abs_path = os.path.abspath(image_path)
            file_input.send_keys(abs_path)
            try:
                WebDriverWait(self.driver, 15).until(
                    lambda d: bool((file_input.get_attribute("value") or "").strip())
                )
            except Exception:
                pass
            time.sleep(2)

            caption = content or title
            caption = self._sanitize_caption(caption)
            if caption:
                try:
                    caption = self._strip_non_bmp(caption)
                    caption_area = form.find_element(By.CSS_SELECTOR, "textarea")
                    try:
                        caption_area.clear()
                    except Exception:
                        pass
                    caption_area.send_keys(caption)
                except Exception:
                    pass

            self._select_radio_option(form=form, name="exp", value="i", label_text="Never expire post")
            self._select_radio_option(form=form, name="com", value="0", label_text="Yes")

            if title:
                try:
                    title = self._strip_non_bmp(title)
                    title_input = form.find_element(By.CSS_SELECTOR, "input[name='title'], #id_title")
                    try:
                        title_input.clear()
                    except Exception:
                        pass
                    title_input.send_keys(title)
                except Exception:
                    pass

            if tags:
                try:
                    tags = self._sanitize_tags(tags)
                    tags_input = form.find_element(By.CSS_SELECTOR, "input[name='tags'], #id_tags")
                    try:
                        tags_input.clear()
                    except Exception:
                        pass
                    tags_input.send_keys(tags)
                except Exception:
                    pass

            submit_btn = form.find_element(
                By.CSS_SELECTOR,
                "button[type='submit'], input[type='submit'], button.btn-primary"
            )
            self.logger.info("Submitting image post...")
            self.driver.execute_script("arguments[0].click();", submit_btn)
            try:
                WebDriverWait(self.driver, 15).until(
                    lambda d: d.current_url != f"{Config.BASE_URL}/share/photo/upload/"
                )
            except TimeoutException:
                pass
            time.sleep(2)

            wait_s = self._detect_rate_limit()
            if wait_s:
                self.logger.warning(f"Rate limit detected. Wait ~{wait_s}s then retry.")
                return {"status": "Rate Limited", "url": ProfileScraper.clean_url(self.driver.current_url), "wait_seconds": wait_s}

            if self._detect_repeating_image():
                self.logger.error("Image rejected: repeating/duplicate image")
                return {"status": "Repeating", "url": ProfileScraper.clean_url(self.driver.current_url)}

            post_url = self._extract_post_url()
            if self._is_denied_or_share_url(post_url):
                self.logger.error(f"Image post denied (url={post_url})")
                return {"status": "Denied", "url": post_url}

            if "damadam.pk" in post_url and ("/comments/image/" in post_url or "/content/" in post_url):
                self.logger.success(f"Image post created: {post_url}")
                return {"status": "Posted", "url": post_url}

            self.logger.warning(f"Post submitted but URL unclear (url={post_url})")
            return {"status": "Pending Verification", "url": post_url}

        except Exception as e:
            self.logger.error(f"Image post error: {e}")
            return {"status": f"Error: {str(e)[:50]}", "url": ""}
        finally:
            try:
                if 'temp_file' in locals() and temp_file and os.path.exists(temp_file):
                    os.unlink(temp_file)
            except Exception:
                pass

    def _download_url_to_temp(self, url: str) -> str:
        last_err = None
        tries = max(1, int(Config.IMAGE_DOWNLOAD_RETRIES))
        for attempt in range(1, tries + 1):
            tmp_path = ""
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=int(Config.IMAGE_DOWNLOAD_TIMEOUT_SECONDS)) as resp:
                    content_type = resp.headers.get("Content-Type", "")
                    suffix = PostQueueLinkPopulator._guess_suffix(url, content_type)
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
                    tmp_path = tmp.name
                    try:
                        while True:
                            chunk = resp.read(1024 * 64)
                            if not chunk:
                                break
                            tmp.write(chunk)
                    finally:
                        tmp.close()

                if os.path.getsize(tmp_path) < 1024:
                    try:
                        with open(tmp_path, "rb") as f:
                            head = f.read(512).lower()
                        if b"<html" in head or b"<!doctype html" in head:
                            try:
                                os.unlink(tmp_path)
                            except Exception:
                                pass
                            raise ValueError("Downloaded HTML instead of image")
                    except Exception:
                        pass

                return tmp_path
            except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
                last_err = e
                try:
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
                if attempt < tries:
                    time.sleep(max(1, int(Config.IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS)) * attempt)
                    continue
                raise
            except Exception as e:
                last_err = e
                try:
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
                raise

        raise last_err if last_err else ValueError("Download failed")

    def _download_drive_file_to_temp(self, file_id: str) -> str:
        if not file_id:
            raise ValueError("Missing Drive file id")

        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        last_err = None
        tries = max(1, int(Config.IMAGE_DOWNLOAD_RETRIES))
        for attempt in range(1, tries + 1):
            try:
                with urllib.request.urlopen(req, timeout=int(Config.IMAGE_DOWNLOAD_TIMEOUT_SECONDS)) as resp:
                    content_type = resp.headers.get("Content-Type", "")

                    if (content_type or "").lower().startswith("text/html"):
                        html = resp.read(1024 * 1024).decode("utf-8", errors="ignore")
                        token_match = re.search(r"confirm=([0-9A-Za-z_]+)", html)
                        token = token_match.group(1) if token_match else ""

                        cookie = resp.headers.get("Set-Cookie", "")
                        if token:
                            url2 = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                            headers = {"User-Agent": "Mozilla/5.0"}
                            if cookie:
                                headers["Cookie"] = cookie
                            req2 = urllib.request.Request(url2, headers=headers)
                            return self._download_url_to_temp(req2.full_url)

                        raise ValueError("Drive download returned HTML")

                return self._download_url_to_temp(url)

            except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
                last_err = e
                if attempt < tries:
                    time.sleep(max(1, int(Config.IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS)) * attempt)
                    continue
                raise

        raise last_err if last_err else ValueError("Drive download failed")

    @staticmethod
    def _extract_drive_file_id(value: str) -> str:
        if not value:
            return ""

        s = value.strip()
        m = re.search(r"/file/d/([^/]+)", s)
        if m:
            return m.group(1)

        try:
            parsed = urlparse(s)
            if parsed.scheme in {"http", "https"}:
                qs = parse_qs(parsed.query)
                if "id" in qs and qs["id"]:
                    return qs["id"][0]
        except Exception:
            pass

        if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s):
            return s

        return ""

    @staticmethod
    def _is_http_url(value: str) -> bool:
        if not value:
            return False
        try:
            u = urlparse(value.strip())
            return u.scheme in {"http", "https"}
        except Exception:
            return False

    def _resolve_image_to_local_path(self, image_path: str) -> (str, bool):
        p = (image_path or "").strip()
        if not p:
            return "", False

        if os.path.exists(p):
            return os.path.abspath(p), False

        drive_id = self._extract_drive_file_id(p)
        if drive_id and ("drive.google.com" in p or "/file/d/" in p or p == drive_id):
            tmp_path = self._download_drive_file_to_temp(drive_id)
            return tmp_path, True

        if self._is_http_url(p):
            tmp_path = self._download_url_to_temp(p)
            return tmp_path, True

        return p, False

    def _select_radio_option(
        self,
        form,
        name: str,
        value: str,
        label_text: str,
        timeout_seconds: int = 5
    ) -> bool:
        try:
            target = None
            radios = form.find_elements(
                By.CSS_SELECTOR,
                f"input[type='radio'][name='{name}'][value='{value}']"
            )
            if radios:
                target = radios[0]
            else:
                try:
                    label = form.find_element(By.XPATH, f".//label[normalize-space()='{label_text}']")
                    for_attr = (label.get_attribute("for") or "").strip()
                    if for_attr:
                        target = form.find_element(By.ID, for_attr)
                    else:
                        target = label
                except Exception:
                    return False

            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target)
            self.driver.execute_script("arguments[0].click();", target)

            WebDriverWait(self.driver, timeout_seconds).until(
                lambda d: any(
                    r.is_selected()
                    for r in form.find_elements(
                        By.CSS_SELECTOR,
                        f"input[type='radio'][name='{name}'][value='{value}']"
                    )
                )
            )
            return True
        except Exception:
            return False

    @staticmethod
    def _collapse_repeats(text: str, max_run: int) -> str:
        if not text:
            return ""
        try:
            n = max(2, int(max_run))
            return re.sub(r"(.)\\1{" + str(n) + r",}", lambda m: m.group(1) * n, text)
        except Exception:
            return text

    @classmethod
    def _sanitize_caption(cls, caption: str) -> str:
        c = (caption or "").strip()
        if not c:
            return ""
        c = cls._collapse_repeats(c, Config.POST_MAX_REPEAT_CHARS)
        max_len = max(1, int(Config.POST_CAPTION_MAX_LEN))
        if len(c) > max_len:
            c = c[:max_len]
        return c

    @classmethod
    def _sanitize_tags(cls, tags: str) -> str:
        t = (tags or "").strip()
        if not t:
            return ""
        t = cls._collapse_repeats(t, Config.POST_MAX_REPEAT_CHARS)
        max_len = max(1, int(Config.POST_TAGS_MAX_LEN))
        if len(t) > max_len:
            t = t[:max_len]
        return t

    @staticmethod
    def _is_denied_or_share_url(url: str) -> bool:
        u = (url or "").strip().lower()
        if not u:
            return True
        if "/share/" in u:
            return True
        if "upload-denied" in u:
            return True
        if "/login" in u or "/signup" in u:
            return True
        return False

    @staticmethod
    def _collapse_repeats(text: str, max_run: int) -> str:
        if not text:
            return ""
        try:
            n = max(2, int(max_run))
            return re.sub(r"(.)\\1{" + str(n) + r",}", lambda m: m.group(1) * n, text)
        except Exception:
            return text

    @classmethod
    def _sanitize_caption(cls, caption: str) -> str:
        c = (caption or "").strip()
        if not c:
            return ""
        c = cls._collapse_repeats(c, Config.POST_MAX_REPEAT_CHARS)
        max_len = max(1, int(Config.POST_CAPTION_MAX_LEN))
        if len(c) > max_len:
            c = c[:max_len]
        return c

    @classmethod
    def _sanitize_tags(cls, tags: str) -> str:
        t = (tags or "").strip()
        if not t:
            return ""
        t = cls._collapse_repeats(t, Config.POST_MAX_REPEAT_CHARS)
        max_len = max(1, int(Config.POST_TAGS_MAX_LEN))
        if len(t) > max_len:
            t = t[:max_len]
        return t

    @staticmethod
    def _extract_drive_file_id(value: str) -> str:
        if not value:
            return ""

        s = value.strip()
        m = re.search(r"/file/d/([^/]+)", s)
        if m:
            return m.group(1)

        try:
            parsed = urlparse(s)
            if parsed.scheme in {"http", "https"}:
                qs = parse_qs(parsed.query)
                if "id" in qs and qs["id"]:
                    return qs["id"][0]
        except Exception:
            pass

        if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s):
            return s

        return ""

    @staticmethod
    def _is_http_url(value: str) -> bool:
        if not value:
            return False
        try:
            u = urlparse(value.strip())
            return u.scheme in {"http", "https"}
        except Exception:
            return False


class PostQueueLinkPopulator:
    def __init__(self, driver, logger: Logger):
        self.driver = driver
        self.logger = logger

    @staticmethod
    def _clean_text(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    @staticmethod
    def _is_http_url(value: str) -> bool:
        if not value:
            return False
        try:
            u = urlparse(value.strip())
            return u.scheme in {"http", "https"}
        except Exception:
            return False

    def _get_first_attr(self, css: str, attr: str) -> str:
        try:
            el = self.driver.find_elements(By.CSS_SELECTOR, css)
            if not el:
                return ""
            return (el[0].get_attribute(attr) or "").strip()
        except Exception:
            return ""

    def _extract_rekhta_image_payload(self) -> Dict[str, str]:
        """Extract minimal fields for PostQueue from a Rekhta shayari-image detail page.

        Returns dict with keys: img_url, title, poet
        """
        img_url = ""
        title = ""
        poet = ""

        # 1) Image URL: share widget has a direct PNG in data-mediasrc
        img_url = self._get_first_attr("div.shareSocial", "data-mediasrc")

        # 2) Fallback: image inside card (often uses data-src)
        if not img_url:
            try:
                imgs = self.driver.find_elements(By.CSS_SELECTOR, "div.shyriImgBox img")
                for img in imgs[:10]:
                    candidate = (img.get_attribute("data-src") or "").strip()
                    if not candidate:
                        candidate = (img.get_attribute("src") or "").strip()
                    if self._is_http_url(candidate) and "rekhta.org/Images/ShayariImages/" in candidate:
                        img_url = candidate
                        break
            except Exception:
                pass

        # 3) Fallback: og:image
        if not img_url:
            try:
                meta = self.driver.find_elements(By.XPATH, "//meta[@property='og:image']")
                if meta:
                    candidate = (meta[0].get_attribute("content") or "").strip()
                    if self._is_http_url(candidate):
                        img_url = candidate
            except Exception:
                pass

        # Title line
        try:
            t = ""
            els = self.driver.find_elements(By.CSS_SELECTOR, "p.shyriImgLine a")
            if els:
                t = els[0].text
            title = self._clean_text(t)
        except Exception:
            title = ""

        # Poet
        try:
            p = ""
            els = self.driver.find_elements(By.CSS_SELECTOR, "h4.shyriImgPoetName a")
            if els:
                p = els[0].text
            poet = self._clean_text(p)
        except Exception:
            poet = ""

        return {"img_url": img_url, "title": title, "poet": poet}

    def collect_rekhta_listing(
        self,
        listing_url: str,
        max_scrolls: int = 6,
        target_count: int = 0,
    ) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []
        seen: set = set()

        if not self.driver:
            return items

        try:
            self.driver.get(listing_url)
            time.sleep(2)
        except Exception:
            return items

        last_count = 0
        last_height = 0
        stable_rounds = 0
        max_scrolls = max(1, int(max_scrolls))
        try:
            tgt = int(target_count or 0)
        except Exception:
            tgt = 0

        def _page_height() -> int:
            try:
                h = self.driver.execute_script("return document.body.scrollHeight || 0;")
                return int(h or 0)
            except Exception:
                return 0

        def _try_click_more() -> bool:
            xpaths = [
                "//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more')]",
                "//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more')]",
                "//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more')]",
                "//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more')]",
            ]
            for xp in xpaths:
                try:
                    el = self.driver.find_element(By.XPATH, xp)
                    if el and el.is_displayed() and el.is_enabled():
                        try:
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
                        except Exception:
                            pass
                        try:
                            el.click()
                        except Exception:
                            try:
                                self.driver.execute_script("arguments[0].click();", el)
                            except Exception:
                                continue
                        return True
                except Exception:
                    continue
            return False

        def _try_next_page_url(page_num: int) -> str:
            try:
                parsed = urlparse(listing_url)
                qs = parse_qs(parsed.query)
                qs["page"] = [str(page_num)]
                new_query = "&".join([f"{k}={quote(str(v[0]))}" for k, v in qs.items() if v])
                return parsed._replace(query=new_query).geturl()
            except Exception:
                if "?" in listing_url:
                    return f"{listing_url}&page={page_num}"
                return f"{listing_url}?page={page_num}"

        def _try_click_next() -> bool:
            selectors = ["a[rel='next']", "a.paginationNext", "a.next"]
            for sel in selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    if el and el.is_displayed() and el.is_enabled():
                        try:
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
                        except Exception:
                            pass
                        try:
                            el.click()
                        except Exception:
                            try:
                                href = (el.get_attribute("href") or "").strip()
                                if href:
                                    self.driver.get(href)
                                else:
                                    self.driver.execute_script("arguments[0].click();", el)
                            except Exception:
                                continue
                        return True
                except Exception:
                    continue
            return False

        scrolls_done = 0
        no_progress_rounds = 0
        observed_per_scroll: List[int] = []
        page_num = 1
        page_advances = 0

        while True:
            cards = []
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, "div.shyriImgBox")
            except Exception:
                cards = []

            for card in cards:
                try:
                    img_link = ""
                    ghazal_link = ""
                    title = ""
                    poet = ""
                    listing_img = ""

                    try:
                        img_link = (card.find_element(By.CSS_SELECTOR, "a.shyriImgInner").get_attribute("href") or "").strip()
                    except Exception:
                        img_link = ""

                    if not img_link or not self._is_http_url(img_link) or img_link in seen:
                        continue

                    try:
                        ghazal_link = (card.find_element(By.CSS_SELECTOR, "p.shyriImgLine a").get_attribute("href") or "").strip()
                    except Exception:
                        ghazal_link = ""

                    try:
                        title = card.find_element(By.CSS_SELECTOR, "p.shyriImgLine a").text
                        title = self._clean_text(title)
                    except Exception:
                        title = ""

                    try:
                        poet = card.find_element(By.CSS_SELECTOR, "h4.shyriImgPoetName a").text
                        poet = self._clean_text(poet)
                    except Exception:
                        poet = ""

                    try:
                        listing_img = (card.find_element(By.CSS_SELECTOR, "div.shareSocial").get_attribute("data-mediasrc") or "").strip()
                    except Exception:
                        listing_img = ""

                    items.append({
                        "image_url": img_link,
                        "ghazal_url": ghazal_link,
                        "title": title,
                        "poet": poet,
                        "listing_img": listing_img
                    })
                    seen.add(img_link)
                except Exception:
                    continue

            if tgt > 0 and len(items) >= tgt:
                break

            prev_count = last_count
            cur_count = len(items)
            added = max(0, cur_count - prev_count)
            if added == 0:
                stable_rounds += 1
            else:
                stable_rounds = 0
                observed_per_scroll.append(added)
            last_count = cur_count

            if tgt > 0 and observed_per_scroll:
                non_zero = [n for n in observed_per_scroll[-5:] if n > 0]
                if non_zero:
                    avg = sum(non_zero) / float(len(non_zero))
                    remaining = max(0, tgt - cur_count)
                    needed = int(math.ceil(remaining / max(1.0, avg))) + 2
                    max_scrolls = max(max_scrolls, scrolls_done + needed)

            if scrolls_done >= max_scrolls:
                break

            before_h = _page_height()
            try:
                self.driver.execute_script("window.scrollBy(0, Math.max(600, window.innerHeight));")
            except Exception:
                pass
            time.sleep(1)
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                pass
            time.sleep(2)
            after_h = _page_height()
            if after_h <= before_h:
                clicked = _try_click_more()
                if clicked:
                    time.sleep(2)
                    after_h = _page_height()

            if added == 0:
                no_progress_rounds += 1
            else:
                no_progress_rounds = 0

            last_height = after_h
            scrolls_done += 1

            if tgt <= 0 and stable_rounds >= 2:
                break

            # If infinite scroll stalls (common around ~50), try pagination.
            if tgt > 0 and no_progress_rounds >= 3 and len(items) < tgt:
                page_num += 1
                did_next = False
                try:
                    did_next = _try_click_next()
                except Exception:
                    did_next = False
                if not did_next:
                    next_url = _try_next_page_url(page_num)
                    try:
                        self.driver.get(next_url)
                        did_next = True
                    except Exception:
                        did_next = False
                if did_next:
                    time.sleep(2)
                    stable_rounds = 0
                    no_progress_rounds = 0
                    scrolls_done = 0
                    observed_per_scroll = []
                    last_count = len(items)
                    page_advances += 1
                    # Guard against infinite paging loops.
                    if page_advances > 50:
                        break
                    continue

            if no_progress_rounds >= 4:
                break

        return items

    def populate(self, sheet, header_map: Dict[str, int], max_rows: int = 0, preview_only: bool = False) -> int:
        if Config.DRY_RUN:
            preview_only = True
        if not self.driver:
            return 0

        src_col_idx = None
        tgt_img_col_idx = None
        tgt_title_ur_col_idx = None
        tgt_title_en_col_idx = None
        for k, v in header_map.items():
            if k in {"POST_LINK", "SOURCE_URL", "SOURCE", "POST_LINK_", "POST LIN"}:
                src_col_idx = v + 1
            if k in {"IMG_LINK", "IMAGE_PATH", "IMAGE", "IMAGE_URL"}:
                tgt_img_col_idx = v + 1
            if k in {"TITLE_UR", "TITLE UR", "CAPTION"}:
                tgt_title_ur_col_idx = v + 1
            if k in {"TITLE_EN", "TITLE EN", "TITLE_ENG", "TITLE", "TITLE_ENG"}:
                tgt_title_en_col_idx = v + 1

        if not src_col_idx:
            self.logger.warning("PostQueue populate skipped: source column (POST_LINK) not found")
            return 0
        if not tgt_img_col_idx:
            self.logger.warning("PostQueue populate skipped: target column (IMG_LINK) not found")
            return 0

        values = sheet.get_all_values()
        # Build a lightweight index of already-known URLs to prevent duplicate work.
        # Only index rows that already have an image URL, so we don't block the current row.
        pq_index = PostQueueIndex()
        for row in values[1:]:
            try:
                src_val = (row[src_col_idx - 1] if len(row) >= src_col_idx else "").strip()
                img_val = (row[tgt_img_col_idx - 1] if len(row) >= tgt_img_col_idx else "").strip()
                if img_val:
                    pq_index.add(img_val)
                    pq_index.add(src_val)
            except Exception:
                continue

        updated = 0
        for r_i, row in enumerate(values[1:], start=2):
            if max_rows and updated >= max_rows:
                break

            source_url = (row[src_col_idx - 1] if len(row) >= src_col_idx else "").strip()
            current_img = (row[tgt_img_col_idx - 1] if len(row) >= tgt_img_col_idx else "").strip()
            if not source_url or current_img:
                continue

            if pq_index.contains(source_url):
                continue

            if not self._is_http_url(source_url):
                continue

            try:
                self.logger.debug(f"Populate Rekhta row={r_i}")
                self.driver.get(source_url)
                time.sleep(2)

                payload = self._extract_rekhta_image_payload()
                img_url = (payload.get("img_url") or "").strip()
                title = (payload.get("title") or "").strip()
                poet = (payload.get("poet") or "").strip()

                caption = title
                if poet:
                    caption = f"{caption} — by {poet}" if caption else f"by {poet}"

                # Preview first: user wants to confirm layout/data before writing sheet
                self.logger.info(
                    f"Rekhta data row={r_i} | img_url={(img_url or 'N/A')[:120]} | title={(title or 'N/A')[:80]} | poet={(poet or 'N/A')[:80]}"
                )

                if img_url and pq_index.contains(img_url):
                    # Duplicate image already exists in PostQueue
                    updated += 1
                    pq_index.add(source_url)
                    continue

                if preview_only:
                    updated += 1
                    continue

                if img_url:
                    sheet.update_cell(r_i, tgt_img_col_idx, img_url)
                else:
                    sheet.update_cell(r_i, tgt_img_col_idx, "IMAGE_NOT_FOUND")

                pq_index.add(source_url)
                if img_url:
                    pq_index.add(img_url)

                if tgt_title_ur_col_idx and caption:
                    sheet.update_cell(r_i, tgt_title_ur_col_idx, caption)
                if tgt_title_en_col_idx and title:
                    sheet.update_cell(r_i, tgt_title_en_col_idx, title)

                updated += 1
                time.sleep(1)
            except Exception as e:
                self.logger.warning(f"Populate IMG_LINK failed row={r_i}: {str(e)[:120]}")
        return updated

    @staticmethod
    def _guess_suffix(url: str, content_type: str) -> str:
        try:
            path = urlparse(url).path
            ext = os.path.splitext(path)[1]
            if ext and len(ext) <= 6:
                return ext
        except Exception:
            pass

        ct = (content_type or "").split(";")[0].strip().lower()
        if ct:
            guess = mimetypes.guess_extension(ct)
            if guess:
                return guess
        return ".jpg"

    def _download_url_to_temp(self, url: str) -> str:
        last_err = None
        tries = max(1, int(Config.IMAGE_DOWNLOAD_RETRIES))
        for attempt in range(1, tries + 1):
            tmp_path = ""
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=int(Config.IMAGE_DOWNLOAD_TIMEOUT_SECONDS)) as resp:
                    content_type = resp.headers.get("Content-Type", "")
                    suffix = self._guess_suffix(url, content_type)
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
                    tmp_path = tmp.name
                    try:
                        while True:
                            chunk = resp.read(1024 * 64)
                            if not chunk:
                                break
                            tmp.write(chunk)
                    finally:
                        tmp.close()

                if os.path.getsize(tmp_path) < 1024:
                    try:
                        with open(tmp_path, "rb") as f:
                            head = f.read(512).lower()
                        if b"<html" in head or b"<!doctype html" in head:
                            try:
                                os.unlink(tmp_path)
                            except Exception:
                                pass
                            raise ValueError("Downloaded HTML instead of image")
                    except Exception:
                        pass

                return tmp_path
            except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
                last_err = e
                try:
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
                if attempt < tries:
                    time.sleep(max(1, int(Config.IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS)) * attempt)
                    continue
                raise
            except Exception as e:
                last_err = e
                try:
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
                raise

        raise last_err if last_err else ValueError("Download failed")

    def _download_drive_file_to_temp(self, file_id: str) -> str:
        if not file_id:
            raise ValueError("Missing Drive file id")

        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        last_err = None
        tries = max(1, int(Config.IMAGE_DOWNLOAD_RETRIES))
        for attempt in range(1, tries + 1):
            try:
                with urllib.request.urlopen(req, timeout=int(Config.IMAGE_DOWNLOAD_TIMEOUT_SECONDS)) as resp:
                    content_type = resp.headers.get("Content-Type", "")

                    if (content_type or "").lower().startswith("text/html"):
                        html = resp.read(1024 * 1024).decode("utf-8", errors="ignore")
                        token_match = re.search(r"confirm=([0-9A-Za-z_]+)", html)
                        token = token_match.group(1) if token_match else ""

                        cookie = resp.headers.get("Set-Cookie", "")
                        if token:
                            url2 = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                            headers = {"User-Agent": "Mozilla/5.0"}
                            if cookie:
                                headers["Cookie"] = cookie
                            req2 = urllib.request.Request(url2, headers=headers)
                            return self._download_url_to_temp(req2.full_url)

                        raise ValueError("Drive download returned HTML")

                return self._download_url_to_temp(url)

            except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
                last_err = e
                if attempt < tries:
                    time.sleep(max(1, int(Config.IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS)) * attempt)
                    continue
                raise

        raise last_err if last_err else ValueError("Drive download failed")

    def _resolve_image_to_local_path(self, image_path: str) -> (str, bool):
        p = (image_path or "").strip()
        if not p:
            return "", False

        if os.path.exists(p):
            return os.path.abspath(p), False

        drive_id = self._extract_drive_file_id(p)
        if drive_id and ("drive.google.com" in p or "/file/d/" in p or p == drive_id):
            tmp_path = self._download_drive_file_to_temp(drive_id)
            return tmp_path, True

        if self._is_http_url(p):
            tmp_path = self._download_url_to_temp(p)
            return tmp_path, True

        return p, False

    def _select_radio_option(
        self,
        form,
        name: str,
        value: str,
        label_text: str,
        timeout_seconds: int = 5
    ) -> bool:
        try:
            target = None
            radios = form.find_elements(
                By.CSS_SELECTOR,
                f"input[type='radio'][name='{name}'][value='{value}']"
            )
            if radios:
                target = radios[0]
            else:
                try:
                    label = form.find_element(By.XPATH, f".//label[normalize-space()='{label_text}']")
                    for_attr = (label.get_attribute("for") or "").strip()
                    if for_attr:
                        target = form.find_element(By.ID, for_attr)
                    else:
                        target = label
                except Exception:
                    return False

            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target)
            self.driver.execute_script("arguments[0].click();", target)

            WebDriverWait(self.driver, timeout_seconds).until(
                lambda d: any(
                    r.is_selected()
                    for r in form.find_elements(
                        By.CSS_SELECTOR,
                        f"input[type='radio'][name='{name}'][value='{value}']"
                    )
                )
            )
            return True
        except Exception:
            return False

    def create_text_post(self, title: str, content: str, tags: str = "") -> Dict:
        """Create a text post"""
        try:
            self.logger.info("Creating text post...")
            self.driver.get(f"{Config.BASE_URL}/share/text/")
            time.sleep(3)

            try:
                form = self._find_share_form(require_file=False)
                if not form:
                    self.logger.error("Text post form not found")
                    return {"status": "Form Error", "url": ""}

                title_input = None
                try:
                    title_input = form.find_element(
                        By.CSS_SELECTOR,
                        "input[name='title'], #id_title, input[name='heading'], input[name='subject']"
                    )
                except Exception:
                    title_input = None

                content_area = form.find_element(
                    By.CSS_SELECTOR,
                    "textarea[name='text'], #id_text, textarea[name='content'], #id_content, textarea"
                )

                submit_btn = form.find_element(
                    By.CSS_SELECTOR,
                    "button[type='submit'], input[type='submit'], button.btn-primary, button.btn"
                )

                # Fill form
                self.logger.debug(f"Title: {title[:50]}...")
                if title_input and title:
                    title = self._strip_non_bmp(title)
                    title_input.clear()
                    title_input.send_keys(title)
                    time.sleep(0.5)

                self.logger.debug(f"Content: {len(content)} chars")
                content = self._strip_non_bmp(content)
                content_area.clear()
                content_area.send_keys(content)
                time.sleep(0.5)

                # Tags if available
                if tags:
                    try:
                        tags = self._sanitize_tags(tags)
                        tags_input = form.find_element(
                            By.CSS_SELECTOR,
                            "input[name='tags'], #id_tags"
                        )
                        tags_input.clear()
                        tags_input.send_keys(tags)
                        self.logger.debug(f"Tags: {tags}")
                    except Exception:
                        self.logger.debug("Tags field not found")

                # Submit
                self.logger.info("Submitting text post...")
                self.driver.execute_script("arguments[0].click();", submit_btn)
                try:
                    WebDriverWait(self.driver, 10).until(lambda d: d.current_url != f"{Config.BASE_URL}/share/text/")
                except TimeoutException:
                    pass
                time.sleep(2)

                # Get result URL
                post_url = self._extract_post_url()

                if self._is_denied_or_share_url(post_url):
                    self.logger.error(f"Text post denied (url={post_url})")
                    return {"status": "Denied", "url": post_url}

                if "damadam.pk" in post_url and "/comments/text/" in post_url:
                    self.logger.success(f"Text post created: {post_url}")
                    return {"status": "Posted", "url": post_url}
                else:
                    self.logger.warning(f"Post submitted but URL unclear (url={post_url})")
                    return {"status": "Pending Verification", "url": post_url}

            except NoSuchElementException as e:
                self.logger.error(f"Form element not found: {e}")
                return {"status": "Form Error", "url": ""}

        except Exception as e:
            self.logger.error(f"Text post error: {e}")
            return {"status": f"Error: {str(e)[:50]}", "url": ""}

    def create_image_post(self, image_path: str, title: str = "", content: str = "", tags: str = "") -> Dict:
        """Create an image post from local file"""
        return self._create_image_post_impl(image_path=image_path, title=title, content=content, tags=tags)

    def _create_image_post_impl(self, image_path: str, title: str = "", content: str = "", tags: str = "") -> Dict:
        """Implementation for image post creation."""
        try:
            self.logger.info("Creating image post...")

            temp_file = ""
            is_temp = False
            try:
                resolved_path, is_temp = self._resolve_image_to_local_path(image_path)
                if is_temp:
                    temp_file = resolved_path
                image_path = resolved_path
            except Exception as e:
                self.logger.error(f"Image download failed: {e}")
                return {"status": "Image Download Failed", "url": ""}

            # Verify file exists
            if not os.path.exists(image_path):
                self.logger.error(f"Image not found: {image_path}")
                return {"status": "File Not Found", "url": ""}

            try:
                size_mb = os.path.getsize(image_path) / (1024 * 1024)
                self.logger.debug(f"Image size: {size_mb:.2f} MB")
            except Exception:
                pass

            self.logger.debug(f"Image: {image_path}")
            self.driver.get(f"{Config.BASE_URL}/share/photo/upload/")
            time.sleep(3)

            try:
                form = self._find_share_form(require_file=True)
                if not form:
                    self.logger.error("Image upload form not found")
                    return {"status": "Form Error", "url": ""}

                # Find file input
                file_input = form.find_element(
                    By.CSS_SELECTOR,
                    "input[type='file'], input[name='file'], input[name='image']"
                )

                # Upload file
                abs_path = os.path.abspath(image_path)
                file_input.send_keys(abs_path)
                self.logger.debug("Image uploaded")
                try:
                    WebDriverWait(self.driver, 15).until(
                        lambda d: bool((file_input.get_attribute("value") or "").strip())
                    )
                except Exception:
                    pass
                time.sleep(2)

                caption = content or title
                caption = self._sanitize_caption(caption)
                if caption:
                    try:
                        caption = self._strip_non_bmp(caption)
                        caption_area = form.find_element(By.CSS_SELECTOR, "textarea")
                        caption_area.clear()
                        caption_area.send_keys(caption)
                    except Exception:
                        pass

                self._select_radio_option(
                    form=form,
                    name="exp",
                    value="i",
                    label_text="Never expire post"
                )
                self._select_radio_option(
                    form=form,
                    name="com",
                    value="0",
                    label_text="Yes"
                )

                # Title if available
                if title:
                    try:
                        title = self._strip_non_bmp(title)
                        title_input = form.find_element(
                            By.CSS_SELECTOR,
                            "input[name='title'], #id_title"
                        )
                        title_input.clear()
                        title_input.send_keys(title)
                        self.logger.debug(f"Title: {title}")
                    except Exception:
                        self.logger.debug("Title field not found")

                # Tags if available
                if tags:
                    try:
                        tags_input = form.find_element(
                            By.CSS_SELECTOR,
                            "input[name='tags'], #id_tags"
                        )
                        tags_input.clear()
                        tags_input.send_keys(tags)
                        self.logger.debug(f"Tags: {tags}")
                    except Exception:
                        self.logger.debug("Tags field not found")

                # Submit
                submit_btn = form.find_element(
                    By.CSS_SELECTOR,
                    "button[type='submit'], input[type='submit'], button.btn-primary"
                )
                self.logger.info("Submitting image post...")
                self.driver.execute_script("arguments[0].click();", submit_btn)
                try:
                    WebDriverWait(self.driver, 15).until(
                        lambda d: d.current_url != f"{Config.BASE_URL}/share/photo/upload/"
                    )
                except TimeoutException:
                    pass
                time.sleep(2)

                # Get result URL
                post_url = self._extract_post_url()

                if self._is_denied_or_share_url(post_url):
                    self.logger.error(f"Image post denied (url={post_url})")
                    return {"status": "Denied", "url": post_url}

                if "damadam.pk" in post_url and ("/comments/image/" in post_url or "/content/" in post_url):
                    self.logger.success(f"Image post created: {post_url}")
                    return {"status": "Posted", "url": post_url}
                else:
                    self.logger.warning(f"Post submitted but URL unclear (url={post_url})")
                    return {"status": "Pending Verification", "url": post_url}

            except NoSuchElementException as e:
                self.logger.error(f"Upload form element not found: {e}")
                return {"status": "Form Error", "url": ""}

        except Exception as e:
            self.logger.error(f"Image post error: {e}")
            return {"status": f"Error: {str(e)[:50]}", "url": ""}

        finally:
            try:
                if 'temp_file' in locals() and temp_file and os.path.exists(temp_file):
                    os.unlink(temp_file)
            except Exception:
                pass

# ============================================================================
# INBOX MONITOR
# ============================================================================

class InboxMonitor:
    """Monitors inbox and manages replies"""

    def __init__(self, driver, logger: Logger):
        self.driver = driver
        self.logger = logger

    def fetch_inbox(self) -> List[Dict]:
        """Fetch all inbox messages"""
        try:
            self.logger.info("Fetching inbox...")
            self.driver.get(f"{Config.BASE_URL}/inbox/")
            time.sleep(3)

            messages: List[Dict] = []

            seen_nicks: set = set()

            blocks = self.driver.find_elements(By.CSS_SELECTOR, "div.mbl.mtl")
            if not blocks:
                self.logger.warning("No inbox items found (check page structure)")
                return []

            for b in blocks[:20]:
                try:
                    nick = ""
                    try:
                        nick_el = b.find_elements(By.CSS_SELECTOR, "div.cl bdi")
                        if nick_el:
                            nick = (nick_el[0].text or "").strip()
                    except Exception:
                        nick = ""

                    if not nick:
                        continue

                    nk = nick.strip().lower()
                    if nk in seen_nicks:
                        continue
                    seen_nicks.add(nk)

                    last_msg = ""
                    timestamp = ""
                    try:
                        line = b.find_elements(By.CSS_SELECTOR, "div.cl.lsp.nos")
                        if line:
                            last_msg = (line[0].text or "").strip()
                            ts_el = line[0].find_elements(By.CSS_SELECTOR, "span[style*='color:#999'], span.cxs")
                            if ts_el:
                                timestamp = (ts_el[-1].text or "").strip()
                    except Exception:
                        pass

                    if not timestamp:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    conv_url = f"{Config.BASE_URL}/inbox/"
                    try:
                        a = b.find_elements(By.CSS_SELECTOR, "a[href*='/comments/'], a[href*='/content/']")
                        if a:
                            href = (a[0].get_attribute("href") or "").strip()
                            if href:
                                conv_url = href if href.startswith("http") else f"{Config.BASE_URL}{href}"
                    except Exception:
                        pass

                    messages.append({
                        "nick": nick,
                        "last_msg": last_msg,
                        "timestamp": timestamp,
                        "conv_url": conv_url
                    })
                except Exception as e:
                    self.logger.debug(f"Skipped inbox item: {e}")
                    continue

            self.logger.success(f"Found {len(messages)} conversations")
            return messages

        except Exception as e:
            self.logger.error(f"Inbox fetch error: {e}")
            return []

    def send_reply(self, conv_url: str, reply_text: str) -> bool:
        """Send reply in a conversation"""
        if Config.DRY_RUN:
            self.logger.info(f"[DRY RUN] Would reply to {conv_url} ({len(reply_text)} chars)")
            return True
        try:
            self.logger.debug(f"Opening conversation: {conv_url}")
            self.driver.get(conv_url)
            time.sleep(3)

            form = None
            forms = self.driver.find_elements(By.CSS_SELECTOR, "form[action*='/direct-response/send']")
            for f in forms:
                try:
                    if f.find_elements(By.CSS_SELECTOR, "textarea[name='direct_response']"):
                        form = f
                        break
                except Exception:
                    continue

            if not form:
                self.driver.get(f"{Config.BASE_URL}/inbox/")
                time.sleep(3)
                forms = self.driver.find_elements(By.CSS_SELECTOR, "form[action*='/direct-response/send']")
                for f in forms:
                    try:
                        if f.find_elements(By.CSS_SELECTOR, "textarea[name='direct_response']"):
                            form = f
                            break
                    except Exception:
                        continue

            if not form:
                self.logger.error("Reply form not found")
                return False

            textarea = form.find_element(By.CSS_SELECTOR, "textarea[name='direct_response']")
            send_btn = None
            try:
                btns = form.find_elements(By.CSS_SELECTOR, "button[type='submit'][name='dec'][value='1']")
                if btns:
                    send_btn = btns[0]
            except Exception:
                send_btn = None
            if not send_btn:
                send_btn = form.find_element(By.CSS_SELECTOR, "button[type='submit']")

            try:
                textarea.clear()
            except Exception:
                pass
            textarea.send_keys(reply_text)
            self.logger.debug(f"Typed reply: {len(reply_text)} chars")
            time.sleep(0.5)

            self.driver.execute_script("arguments[0].click();", send_btn)
            self.logger.info("Reply sent")
            time.sleep(3)

            try:
                if reply_text in (self.driver.page_source or ""):
                    self.logger.success("Reply verified")
                    return True
            except Exception:
                pass
            self.logger.warning("Reply sent but not verified")
            return True

        except Exception as e:
            self.logger.error(f"Reply error: {e}")
            return False

    def fetch_activity(self, max_items: int = 20, max_pages: int = 3) -> List[Dict]:
        try:
            items: List[Dict] = []
            seen: set = set()

            for page in range(1, max_pages + 1):
                if len(items) >= max_items:
                    break

                url = f"{Config.BASE_URL}/inbox/activity/" if page == 1 else f"{Config.BASE_URL}/inbox/activity/?page={page}"
                self.driver.get(url)
                time.sleep(3)

                blocks = self.driver.find_elements(By.CSS_SELECTOR, "div.mbl.mtl")
                if not blocks:
                    break

                for b in blocks:
                    if len(items) >= max_items:
                        break
                    try:
                        raw = (b.text or "").strip()
                        if not raw:
                            continue

                        lines = []
                        for ln in raw.splitlines():
                            s = (ln or "").strip()
                            if not s:
                                continue
                            if s in {"►", "REMOVE"}:
                                continue
                            lines.append(s)

                        t = "\n".join(lines).strip()
                        if not t:
                            continue

                        item_url = ""
                        try:
                            a = b.find_elements(By.CSS_SELECTOR, "a[href*='/comments/'], a[href*='/content/']")
                            if a:
                                href = (a[0].get_attribute("href") or "").strip()
                                if href:
                                    item_url = href if href.startswith("http") else f"{Config.BASE_URL}{href}"
                        except Exception:
                            pass

                        key = (t[:200], item_url)
                        if key in seen:
                            continue
                        seen.add(key)
                        items.append({"text": t[:500], "url": item_url})
                    except Exception:
                        continue

                try:
                    next_btn = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='?page='] button")
                    has_next = False
                    for btn in next_btn:
                        try:
                            if "NEXT" in ((btn.text or "").upper()):
                                has_next = True
                                break
                        except Exception:
                            continue
                    if not has_next:
                        break
                except Exception:
                    break

            return items
        except Exception:
            return []

    def get_conversation_log(self, conv_url: str) -> str:
        """Get full conversation history as text"""
        try:
            self.driver.get(conv_url)
            time.sleep(2)

            # Find all messages
            messages = self.driver.find_elements(
                By.CSS_SELECTOR,
                ".message, article, div[class*='msg']"
            )

            log_lines = []
            for msg in messages:
                try:
                    sender = msg.find_element(
                        By.CSS_SELECTOR,
                        "b, .sender, strong"
                    ).text.strip()

                    text = msg.find_element(
                        By.CSS_SELECTOR,
                        "bdi, .text, span, p"
                    ).text.strip()

                    if sender and text:
                        log_lines.append(f"{sender}: {text}")
                except Exception:
                    continue

            return "\n".join(log_lines)

        except Exception as e:
            self.logger.error(f"Conversation log error: {e}")
            return ""

# ============================================================================
# PHASE 1: MESSAGE MODE
# ============================================================================

def run_message_mode(args):
    """Phase 1: Send personal messages to targets"""
    logger = Logger("msg")
    _print_sheet_context(logger)
    logger.info("=" * 70)
    logger.info(f"DamaDam Bot V{VERSION} - MESSAGE MODE")
    logger.info("=" * 70 + "\n")

    browser_mgr = BrowserManager(logger)
    driver = browser_mgr.setup()
    if not driver:
        logger.error("Browser setup failed")
        return

    try:
        # Login
        logger.info("🔐 Login...")
        if not browser_mgr.login():
            logger.error("Login failed - check credentials")
            return
        logger.success("✅ Login Success\n")

        # Connect to Google Sheets
        logger.info("📊 Connecting to Google Sheets...")
        sheets_mgr = SheetsManager(logger)
        if not sheets_mgr.connect():
            logger.error("Sheets connection failed")
            return
        logger.success("✅ Sheet Connected\n")

        activity = ActivityLogger(sheets_mgr, logger)
        activity.initialize()
        conv_logger = ConversationLogger(sheets_mgr, logger)
        conv_logger.initialize()

        # Initialize components
        scraper = ProfileScraper(driver, logger)
        recorder = MessageRecorder(sheets_mgr, logger)
        if not recorder.initialize():
            logger.warning("Message history tracking unavailable")
        sender = MessageSender(driver, logger, scraper, recorder)

        # Get MsgList sheet
        msglist = sheets_mgr.get_sheet(Config.SHEET_ID, "MsgList")
        if not msglist:
            logger.error("MsgList sheet not found")
            return

        # Load pending targets
        logger.info("📋 Loading pending targets...")
        sheets_mgr.api_calls += 1
        all_rows = msglist.get_all_values()

        headers = all_rows[0] if all_rows else []
        header_map: Dict[str, int] = {}
        for idx, h in enumerate(headers, start=1):
            key = (h or "").strip().lower()
            if key:
                header_map[key] = idx

        def _col(*names: str, default: Optional[int] = None) -> Optional[int]:
            for n in names:
                k = (n or "").strip().lower()
                if k in header_map:
                    return header_map[k]
            return default

        gender_col = _col("gender", "Gender")
        mode_col = _col("mode", default=1)
        name_col = _col("name", default=2)
        nick_col = _col("nick/url", "nick", "nick/url ", "nick/url", default=3)
        city_col = _col("city", default=4)
        posts_col = _col("posts", default=5)
        followers_col = _col("followers", default=6)
        message_col = _col("message", default=8 if gender_col else 7)
        status_col = _col("status", default=9 if gender_col else 8)
        notes_col = _col("notes", default=10 if gender_col else 9)
        result_url_col = _col("result url", "result_url", "resulturl", default=11 if gender_col else 10)

        pending = []
        pending_status_rows = 0
        pending_missing_message = 0
        pending_missing_nick = 0
        for i, row in enumerate(all_rows[1:], start=2):
            def _cell(col_idx: Optional[int]) -> str:
                if not col_idx:
                    return ""
                j = col_idx - 1
                if j < 0 or j >= len(row):
                    return ""
                return (row[j] or "").strip()

            mode = _cell(mode_col).lower()
            name = _cell(name_col)
            nick_or_url = _cell(nick_col)
            city = _cell(city_col)
            posts = _cell(posts_col)
            followers = _cell(followers_col)
            gender = _cell(gender_col)
            message = _cell(message_col)
            status = _cell(status_col).lower()
            notes = _cell(notes_col)

            if status.startswith("pending"):
                pending_status_rows += 1
                if not nick_or_url:
                    pending_missing_nick += 1
                    continue
                if not message:
                    pending_missing_message += 1
                    continue

                pending.append({
                    "row": i,
                    "mode": mode,
                    "name": name,
                    "nick_or_url": nick_or_url,
                    "city": city,
                    "posts": posts,
                    "followers": followers,
                    "gender": gender,
                    "message": message,
                    "notes": notes
                })

        if not pending:
            logger.warning("⚠️ No pending targets found in MsgList")
            if pending_status_rows:
                logger.info(
                    f"Found {pending_status_rows} rows with STATUS='pending' but none were runnable "
                    f"(missing NICK/URL={pending_missing_nick}, missing MESSAGE={pending_missing_message})"
                )
            else:
                logger.info("Add targets with STATUS='pending' in MsgList sheet")
            return

        # Apply max limit
        if Config.MAX_PROFILES > 0:
            pending = pending[:Config.MAX_PROFILES]
            logger.info(f"📌 Limited to {Config.MAX_PROFILES} targets")

        logger.success(f"✅ Found {len(pending)} pending targets\n")
        logger.info("=" * 70 + "\n")

        # Process each target
        success_count = 0
        failed_count = 0

        for idx, target in enumerate(pending, 1):
            logger.info("\n" + "─" * 70)
            logger.info(f"[{idx}/{len(pending)}] 👤 Processing: {target['name']}")
            logger.info("─" * 70)

            try:
                mode = target["mode"]
                name = target["name"]
                nick_or_url = target["nick_or_url"]
                message = target["message"]
                row_num = target["row"]

                post_url = None
                profile = {
                    "NAME": name,
                    "NICK": nick_or_url,
                    "CITY": target.get("city", ""),
                    "POSTS": target.get("posts", "0"),
                    "FOLLOWERS": target.get("followers", "0"),
                    "GENDER": target.get("gender", "")
                }

                # Handle MODE
                if mode == "url":
                    # Direct URL mode
                    post_url = ProfileScraper.clean_url(nick_or_url)
                    if not ProfileScraper.is_valid_url(post_url):
                        raise ValueError(f"Invalid URL: {nick_or_url}")
                    logger.info("🌐 Mode: Direct URL")
                    logger.info(f"   Target: {post_url}")

                else:
                    # Nick mode - scrape profile first
                    logger.info("👤 Mode: Nickname")
                    logger.info(f"   Target: {nick_or_url}")

                    profile = scraper.scrape_profile(nick_or_url)
                    if not profile:
                        logger.error("❌ Profile scrape failed")
                        sheets_mgr.update_cell(msglist, row_num, status_col or 8, "Failed")
                        sheets_mgr.update_cell(msglist, row_num, notes_col or 9, "Profile scrape failed")
                        try:
                            activity.log(
                                mode="msg",
                                action="profile_scrape_failed",
                                nick=nick_or_url,
                                url="",
                                status="Failed",
                                details="Profile scrape failed"
                            )
                        except Exception:
                            pass
                        failed_count += 1
                        continue

                    # Check if suspended
                    if profile.get("STATUS") == "Suspended":
                        logger.warning("⚠️ Account suspended")
                        sheets_mgr.update_cell(msglist, row_num, status_col or 8, "Skipped")
                        sheets_mgr.update_cell(msglist, row_num, notes_col or 9, "Account suspended")
                        try:
                            activity.log(
                                mode="msg",
                                action="account_suspended",
                                nick=profile.get("NICK", nick_or_url),
                                url="",
                                status="Skipped",
                                details="Account suspended"
                            )
                        except Exception:
                            pass
                        failed_count += 1
                        continue

                    # Update sheet with profile data
                    if profile.get("CITY"):
                        sheets_mgr.update_cell(msglist, row_num, city_col or 4, profile["CITY"])
                    if profile.get("POSTS"):
                        sheets_mgr.update_cell(msglist, row_num, posts_col or 5, profile["POSTS"])
                    if profile.get("FOLLOWERS"):
                        sheets_mgr.update_cell(msglist, row_num, followers_col or 6, profile["FOLLOWERS"])
                    if gender_col and profile.get("GENDER"):
                        sheets_mgr.update_cell(msglist, row_num, gender_col, profile["GENDER"])

                    # Check post count
                    post_count = int(profile.get("POSTS", "0"))
                    if post_count == 0:
                        logger.warning("⚠️ No posts available")
                        sheets_mgr.update_cell(msglist, row_num, status_col or 8, "Skipped")
                        sheets_mgr.update_cell(msglist, row_num, notes_col or 9, "No posts")
                        try:
                            activity.log(
                                mode="msg",
                                action="no_posts",
                                nick=profile.get("NICK", nick_or_url),
                                url="",
                                status="Skipped",
                                details="No posts"
                            )
                        except Exception:
                            pass
                        failed_count += 1
                        continue

                    # Find open post (text or image)
                    logger.info("🔍 Finding open post...")
                    post_url = scraper.find_open_post(nick_or_url, post_type="any")
                    if not post_url:
                        logger.error("❌ No open posts found")

                        max_pages = Config.MAX_POST_PAGES if Config.MAX_POST_PAGES > 0 else 4
                        sheets_mgr.update_cell(msglist, row_num, status_col or 8, "Failed")
                        sheets_mgr.update_cell(
                            msglist,
                            row_num,
                            notes_col or 9,
                            f"No open posts found (scanned up to {max_pages} pages)"
                        )

                        try:
                            activity.log(
                                mode="msg",
                                action="no_open_posts",
                                nick=profile.get("NICK", nick_or_url),
                                url="",
                                status="Failed",
                                details=f"scanned_pages={max_pages}"
                            )
                        except Exception:
                            pass

                        failed_count += 1
                        continue

                # Process message template
                processed_msg = sender.process_template(message, profile)
                logger.info(f"💬 Message: '{processed_msg}' ({len(processed_msg)} chars)")

                # Send message
                result = sender.send_message(post_url, processed_msg, nick_or_url)

                try:
                    nick_for_logs = ""
                    if isinstance(profile, dict):
                        nick_for_logs = (profile.get("NICK") or "").strip()
                    if not nick_for_logs:
                        nick_for_logs = nick_or_url

                    base_url = ProfileScraper.clean_url(post_url or "")
                    activity.log(
                        mode="msg",
                        action="send_message",
                        nick=nick_for_logs,
                        url=base_url,
                        status=result.get("status", ""),
                        details=f"target_mode={mode}; result_url={ProfileScraper.clean_url(result.get('url',''))}"
                    )
                    conv_logger.log(
                        nick=nick_for_logs,
                        direction="OUT",
                        mode="msg",
                        message=processed_msg,
                        url=base_url,
                        status=result.get("status", "")
                    )
                except Exception:
                    pass

                # Update sheet based on result
                timestamp = datetime.now().strftime("%I:%M %p")
                if result.get("status") == "Dry Run" or "Posted" in result["status"]:
                    logger.success("✅ SUCCESS - Message posted!")
                    logger.info(f"🔗 URL: {result['url']}")
                    sheets_mgr.update_cell(msglist, row_num, status_col or 8, "Done")
                    sheets_mgr.update_cell(msglist, row_num, notes_col or 9, f"Posted @ {timestamp}")
                    sheets_mgr.update_cell(msglist, row_num, result_url_col or 10, result["url"])
                    success_count += 1

                elif "Verification" in result["status"]:
                    logger.warning("⚠️ Needs manual verification")
                    logger.info(f"🔗 Check: {result['url']}")
                    sheets_mgr.update_cell(msglist, row_num, status_col or 8, "Done")
                    sheets_mgr.update_cell(msglist, row_num, notes_col or 9, f"Verify @ {timestamp}")
                    sheets_mgr.update_cell(msglist, row_num, result_url_col or 10, result["url"])
                    success_count += 1

                else:
                    logger.error(f"❌ FAILED - {result['status']}")
                    sheets_mgr.update_cell(msglist, row_num, status_col or 8, "Failed")
                    sheets_mgr.update_cell(msglist, row_num, notes_col or 9, result["status"])
                    if result.get("url"):
                        sheets_mgr.update_cell(msglist, row_num, result_url_col or 10, result["url"])
                    failed_count += 1

                # Rate limiting
                time.sleep(2)

            except Exception as e:
                error_msg = str(e)[:60]
                logger.error(f"❌ Error: {error_msg}")
                sheets_mgr.update_cell(msglist, target["row"], status_col or 8, "Failed")
                sheets_mgr.update_cell(msglist, target["row"], notes_col or 9, error_msg)
                failed_count += 1

                try:
                    activity.log(
                        mode="msg",
                        action="exception",
                        nick=target.get("nick_or_url", ""),
                        url="",
                        status="Exception",
                        details=error_msg
                    )
                except Exception:
                    pass

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("📊 MESSAGE MODE SUMMARY")
        logger.info("=" * 70)
        logger.success(f"✅ Success: {success_count}/{len(pending)}")
        logger.error(f"❌ Failed: {failed_count}/{len(pending)}")
        logger.info(f"📞 API Calls: {sheets_mgr.api_calls}")
        logger.info(f"📝 Log: {logger.log_file}")
        logger.info("=" * 70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        browser_mgr.close()

# ============================================================================
# PHASE 1.5: REKHTA POPULATE MODE
# ============================================================================

def run_populate_mode(args):
    """Populate PostQueue from Rekhta shayari-image listing."""
    logger = Logger("populate")
    _print_sheet_context(logger)
    try:
        console.print(
            Panel.fit(
                f"DamaDam Bot V{VERSION} - POPULATE MODE",
                title="POPULATE",
                border_style="cyan",
            )
        )
    except Exception:
        logger.info("=" * 70)
        logger.info(f"DamaDam Bot V{VERSION} - POPULATE MODE")
        logger.info("=" * 70 + "\n")

    browser_mgr = BrowserManager(logger)
    driver = browser_mgr.setup()
    if not driver:
        return

    try:
        sheets_mgr = SheetsManager(logger)
        if not sheets_mgr.connect():
            return

        activity = ActivityLogger(sheets_mgr, logger)
        activity.initialize()

        post_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "PostQueue")
        if not post_queue:
            return

        if not sheets_mgr.ensure_postqueue_headers(post_queue):
            return

        sheets_mgr.api_calls += 1
        all_rows = post_queue.get_all_values()

        headers = all_rows[0] if all_rows else []
        if not headers:
            logger.warning("PostQueue headers missing. Please add header row first.")
            return

        header_map: Dict[str, int] = {}
        for idx, h in enumerate(headers):
            key = (h or "").strip().upper()
            if key and key not in header_map:
                header_map[key] = idx

        def find_col(*keys: str) -> Optional[int]:
            for k in keys:
                kk = (k or "").strip().upper()
                if kk in header_map:
                    return header_map[kk]
            return None

        col_status = find_col("STATUS", "STATU")
        col_title = find_col("TITLE", "TITLE_EN", "TITLE EN", "TITLE_ENG")
        col_title_ur = find_col("TITLE_UR", "TITLE UR", "CAPTION")
        col_image_path = find_col("IMAGE_PATH", "IMG_LINK", "IMAGE", "IMAGE_URL")
        col_type = find_col("TYPE")
        col_timestamp = find_col("TIMESTAMP", "TIME")
        col_notes = find_col("NOTES", "NOTE")
        col_signature = find_col("SIGNATURE")

        if col_image_path is None or col_title is None:
            logger.warning("PostQueue needs IMAGE_PATH and TITLE columns to populate.")
            return

        # ── Collect ALL existing image URLs AND source listing URLs from sheet ──
        # We also look for a SOURCE_URL column to track which Rekhta page was scraped
        col_source_url = find_col("SOURCE_URL", "REKHTA_URL", "SOURCE")

        # Remove duplicate IMAGE_PATH rows (keep first occurrence)
        if col_image_path is not None:
            seen = set()
            delete_rows = []
            for idx, row in enumerate(all_rows[1:], start=2):
                if len(row) <= col_image_path:
                    continue
                img_val = (row[col_image_path] or "").strip().lower()
                if not img_val:
                    continue
                if img_val in seen:
                    delete_rows.append(idx)
                else:
                    seen.add(img_val)

            if delete_rows:
                logger.warning(f"Found {len(delete_rows)} duplicate IMAGE_PATH rows. Removing...")
                for r in reversed(delete_rows):
                    sheets_mgr.delete_rows(post_queue, r)
                sheets_mgr.api_calls += 1
                all_rows = post_queue.get_all_values()

        # ── Build unified duplicate index from PostQueue ──
        pq_index = PostQueueIndex.from_postqueue_values(all_rows, header_map)

        listing_url = (Config.REKHTA_LISTING_URL or "").strip()
        if not listing_url:
            logger.warning("Rekhta listing URL missing (DD_REKHTA_LISTING_URL).")
            return

        pop = PostQueueLinkPopulator(driver, logger)
        logger.info(f"Opening Rekhta listing: {listing_url}")

        limit = 0
        if Config.REKHTA_POPULATE_LIMIT > 0:
            limit = Config.REKHTA_POPULATE_LIMIT
        elif Config.MAX_PROFILES > 0:
            limit = Config.MAX_PROFILES

        items = pop.collect_rekhta_listing(listing_url, Config.REKHTA_MAX_SCROLLS, target_count=limit)
        if not items:
            logger.warning("No Rekhta items found on listing.")
            return
        if limit > 0:
            items = items[:limit]

        preview_only = not (Config.REKHTA_POPULATE_WRITE or getattr(args, "populate_write", False))
        if preview_only:
            logger.info("Preview mode ON (no sheet updates). Set DD_REKHTA_POPULATE_WRITE=1 to write.")

        total = len(items)
        added = 0
        skipped = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}", markup=False),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
            refresh_per_second=10,
        ) as progress:
            task = progress.add_task("Rekhta items", total=None)
            try:
                for idx, item in enumerate(items, 1):
                    progress.update(task, advance=1, description=f"[{idx}/{total}] Rekhta")
                    source_url = (item.get("image_url") or "").strip()
                    if not source_url or not pop._is_http_url(source_url):
                        skipped += 1
                        try:
                            activity.log(
                                mode="populate",
                                action="skip_invalid_source_url",
                                nick="",
                                url=source_url,
                                status="skipped",
                                details="invalid listing url"
                            )
                        except Exception:
                            pass
                        continue

                    src_key = source_url.lower()
                    if pq_index.contains(src_key):
                        skipped += 1
                        try:
                            activity.log(
                                mode="populate",
                                action="skip_duplicate",
                                nick="",
                                url=source_url,
                                status="skipped",
                                details="duplicate"
                            )
                        except Exception:
                            pass
                        continue

                    # Fast-path duplicate check: the listing page often exposes the final image URL.
                    # If it's already present in PostQueue, skip opening the detail page.
                    listing_img = (item.get("listing_img") or "").strip()
                    listing_img_key = listing_img.lower() if listing_img else ""
                    if listing_img_key and pq_index.contains(listing_img_key):
                        skipped += 1
                        logger.info(f"  ↳ Skipped: IMAGE_PATH already exists in PostQueue")
                        try:
                            activity.log(
                                mode="populate",
                                action="skip_duplicate_img",
                                nick="",
                                url=listing_img,
                                status="skipped",
                                details="listing_img duplicate"
                            )
                        except Exception:
                            pass
                        pq_index.add(src_key)
                        continue

                    try:
                        driver.get(source_url)
                        time.sleep(2)
                    except Exception:
                        skipped += 1
                        try:
                            activity.log(
                                mode="populate",
                                action="open_source_failed",
                                nick="",
                                url=source_url,
                                status="failed",
                                details="driver.get failed"
                            )
                        except Exception:
                            pass
                        continue

                    payload = pop._extract_rekhta_image_payload()
                    img_url = (payload.get("img_url") or item.get("listing_img") or "").strip()
                    title = (payload.get("title") or item.get("title") or "").strip()
                    poet = (payload.get("poet") or item.get("poet") or "").strip()

                    caption = title
                    if poet:
                        caption = f"{caption} — by {poet}" if caption else f"by {poet}"

                    logger.info(
                        f"Rekhta data {idx}/{total} | img_url={(img_url or 'N/A')[:120]} | title={(title or 'N/A')[:80]} | poet={(poet or 'N/A')[:80]}"
                    )

                    # ── Block duplicate final image URL before writing ──
                    img_key = (img_url or "").strip().lower()
                    if img_key and pq_index.contains(img_key):
                        skipped += 1
                        logger.info(f"  ↳ Skipped: IMAGE_PATH already exists in PostQueue")
                        try:
                            activity.log(mode="populate", action="skip_duplicate_img",
                                         nick="", url=img_url, status="skipped", details="img_url duplicate")
                        except Exception:
                            pass
                        continue

                    if preview_only:
                        added += 1
                        pq_index.add(src_key)
                        if img_key:
                            pq_index.add(img_key)
                        try:
                            activity.log(
                                mode="populate", action="preview_item", nick="",
                                url=source_url, status="preview",
                                details=f"title={(title or '')[:120]}; poet={(poet or '')[:120]}"
                            )
                        except Exception:
                            pass
                        if limit > 0 and added >= limit:
                            break
                        continue

                    row = ["" for _ in range(len(headers))]
                    if col_status is not None:
                        row[col_status] = "pending"
                    if col_title is not None and title:
                        row[col_title] = title
                    if col_title_ur is not None and caption:
                        row[col_title_ur] = caption
                    if col_image_path is not None:
                        row[col_image_path] = img_url or "IMAGE_NOT_FOUND"
                    if col_type is not None:
                        row[col_type] = "image"
                    if col_timestamp is not None:
                        row[col_timestamp] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if col_notes is not None:
                        row[col_notes] = "rekhta"
                    if col_signature is not None:
                        row[col_signature] = ""
                    # Write source URL if column exists
                    if col_source_url is not None:
                        row[col_source_url] = source_url

                    sheets_mgr.append_row(post_queue, row)
                    pq_index.add(src_key)
                    if img_key:
                        pq_index.add(img_key)
                    added += 1
                    try:
                        activity.log(
                            mode="populate",
                            action="add_postqueue_row",
                            nick="",
                            url=source_url,
                            status="added",
                            details=f"img={(img_url or '')[:120]}; title={(title or '')[:120]}; poet={(poet or '')[:120]}"
                        )
                    except Exception:
                        pass
                    if limit > 0 and added >= limit:
                        break
            except KeyboardInterrupt:
                logger.warning("\n⚠️ Interrupted by user")

        logger.info("\n" + "=" * 70)
        if preview_only:
            logger.success(f"✅ Previewed: {added}/{total}")
        else:
            logger.success(f"✅ Added: {added}/{total}")
        if skipped:
            logger.warning(f"⚠️ Skipped: {skipped}")
        logger.info(f"📞 API Calls: {sheets_mgr.api_calls}")
        logger.info(f"📝 Log: {logger.log_file}")
        logger.info("=" * 70 + "\n")

    finally:
        browser_mgr.close()

# ============================================================================
# PHASE 2: POST MODE
# ============================================================================

def run_post_mode(args):
    """Phase 2: Create new posts (text/image)"""
    logger = Logger("post")
    _print_sheet_context(logger)
    try:
        console.print(
            Panel.fit(
                f"DamaDam Bot V{VERSION} - POST MODE",
                title="POST",
                border_style="cyan",
            )
        )
    except Exception:
        logger.info("=" * 70)
        logger.info(f"DamaDam Bot V{VERSION} - POST MODE")
        logger.info("=" * 70 + "\n")

    browser_mgr = BrowserManager(logger)
    driver = browser_mgr.setup()
    if not driver:
        return

    try:
        logger.info("🔐 Login...")
        if not browser_mgr.login():
            logger.error("Login failed - check credentials")
            return
        logger.success("✅ Login Success\n")

        sheets_mgr = SheetsManager(logger)
        logger.info("📊 Connecting to Google Sheets...")
        if not sheets_mgr.connect():
            logger.error("Sheets connection failed")
            return
        logger.success("✅ Sheet Connected\n")

        activity = ActivityLogger(sheets_mgr, logger)
        activity.initialize()
        conv_logger = ConversationLogger(sheets_mgr, logger)
        conv_logger.initialize()
        post_history = PostHistoryRecorder(sheets_mgr, logger)
        post_history.initialize()

        creator = PostCreator(driver, logger)
        post_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "PostQueue")
        if not post_queue:
            return

        if not sheets_mgr.ensure_postqueue_headers(post_queue):
            return

        logger.info("📋 Loading pending posts...")
        sheets_mgr.api_calls += 1
        all_rows = post_queue.get_all_values()

        headers = all_rows[0] if all_rows else []
        header_map: Dict[str, int] = {}
        for idx, h in enumerate(headers):
            key = (h or "").strip().upper()
            if key and key not in header_map:
                header_map[key] = idx

        if getattr(args, "populate_img_links", False) or Config.POPULATE_IMG_LINKS:
            try:
                pop = PostQueueLinkPopulator(driver, logger)
                preview_only = not bool(Config.POPULATE_IMG_LINKS_WRITE)
                n = pop.populate(post_queue, header_map, preview_only=preview_only)
                if n:
                    if preview_only:
                        logger.info(f"Previewed Rekhta data for {n} rows (no sheet updates)")
                    else:
                        logger.info(f"Updated PostQueue (IMG_LINK/title fields) for {n} rows")
                        sheets_mgr.api_calls += 1
                        all_rows = post_queue.get_all_values()
            except Exception as e:
                logger.warning(f"IMG_LINK population skipped: {str(e)[:120]}")

        use_headers = bool(header_map)

        def get_cell(row: List[str], key: str) -> str:
            if not use_headers:
                return ""
            k = (key or "").strip().upper()
            if k not in header_map:
                return ""
            j = header_map[k]
            if j < 0 or j >= len(row):
                return ""
            return (row[j] or "").strip()

        def get_any(row: List[str], *keys: str) -> str:
            for k in keys:
                val = get_cell(row, k)
                if val:
                    return val
            return ""

        # Build an index of already-processed posts to avoid re-posting duplicates.
        # We only consider rows that are NOT pending as "already done".
        posted_index = PostQueueIndex()
        if use_headers and all_rows:
            for r in all_rows[1:]:
                try:
                    st = get_any(r, "STATUS", "STATU").lower()
                    if st.startswith("pending"):
                        continue
                    img_val = get_any(r, "IMAGE_PATH", "IMG_LINK", "IMAGE", "IMAGE_URL")
                    post_url_val = get_any(r, "POST_URL", "RESULT_URL", "RESULT URL")
                    if img_val:
                        posted_index.add(img_val)
                    if post_url_val:
                        posted_index.add(post_url_val)
                except Exception:
                    continue

        def find_col(*keys: str, default_1_based: int) -> int:
            if not use_headers:
                return default_1_based
            for k in keys:
                kk = (k or "").strip().upper()
                if kk in header_map:
                    return header_map[kk] + 1
            return default_1_based

        def get_legacy(row: List[str], idx: int) -> str:
            if idx < 0 or idx >= len(row):
                return ""
            return (row[idx] or "").strip()

        if header_map:
            col_status = find_col("STATUS", "STATU", default_1_based=6)
            col_post_url = find_col("POST_URL", "RESULT_URL", "RESULT URL", default_1_based=7)
            col_timestamp = find_col("TIMESTAMP", "TIME", default_1_based=8)
            col_notes = find_col("NOTES", "NOTE", default_1_based=9)
        else:
            # Fallback to legacy layout if headers are missing
            col_status = 6
            col_post_url = 7
            col_timestamp = 8
            col_notes = 9

        pending = []
        dup_skipped = 0
        for i, row in enumerate(all_rows[1:], start=2):
            if use_headers:
                post_type = get_any(row, "TYPE").lower()
                status = get_any(row, "STATUS", "STATU").lower()

                title_en = get_any(row, "TITLE", "TITLE_EN", "TITLE_ENG", "TITLE EN")
                title_ur = get_any(row, "TITLE_UR", "TITLE_URDU", "TITLE UR", "CAPTION")
                img_link = get_any(row, "IMAGE_PATH", "IMG_LINK", "IMAGE", "IMAGE_URL")

                # User-defined PostQueue rules:
                # - TYPE=text: use column TITLE as content
                # - TYPE=image + STATUS=pending: use IMAGE_PATH as image URL and TITLE_UR as caption
                if post_type == "text":
                    title = ""
                    content = title_en
                    image_path = ""
                else:
                    title = title_en
                    content = title_ur
                    image_path = img_link

                tags = get_any(row, "TAGS")
                notes_val = get_any(row, "NOTES", "NOTE")
            else:
                # Legacy layout: TYPE, TITLE, CONTENT, IMAGE_PATH, TAGS, STATUS, ...
                post_type = get_legacy(row, 0).lower()
                title = get_legacy(row, 1)
                content = get_legacy(row, 2)
                image_path = get_legacy(row, 3)
                tags = get_legacy(row, 4)
                status = get_legacy(row, 5).lower()
                notes_val = get_legacy(row, 8)

            if not post_type:
                continue

            should_run = status.startswith("pending")
            attempt_num = 1

            if not should_run:
                continue

            # Unified duplicate skip for image posts:
            # If the same IMAGE_PATH already appears in a non-pending row, don't attempt to post again.
            if use_headers and post_type != "text" and image_path and posted_index.contains(image_path):
                dup_skipped += 1
                try:
                    sheets_mgr.update_cell(post_queue, i, col_status, "Skipped Duplicate")
                    sheets_mgr.update_cell(post_queue, i, col_notes, "Duplicate IMAGE_PATH already processed")
                except Exception:
                    pass
                continue

            pending.append({
                "row": i,
                "type": post_type,
                "title": title,
                "content": content,
                "image_path": image_path,
                "tags": tags,
                "attempt": attempt_num
            })

        if not pending:
            logger.warning("No pending posts in PostQueue")
            return

        if dup_skipped:
            logger.info(f"Skipped {dup_skipped} pending rows due to duplicate IMAGE_PATH")

        if Config.MAX_PROFILES > 0:
            pending = pending[:Config.MAX_PROFILES]

        logger.success(f"Found {len(pending)} pending posts\n")

        success = 0
        failed = 0
        skipped = 0

        consecutive_denied = 0
        stop_reason = ""

        def cooldown_wait(seconds: int):
            if seconds <= 0:
                return
            try:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("{task.description}", markup=False),
                    BarColumn(),
                    TimeElapsedColumn(),
                    console=console,
                    refresh_per_second=10,
                ) as cd:
                    t = cd.add_task(f"Cooldown {seconds}s", total=None)
                    for _ in range(seconds):
                        time.sleep(1)
                        cd.advance(t, 1)
            except Exception:
                time.sleep(seconds)

        def _jitter_seconds(base: float, jitter: float) -> float:
            try:
                b = float(base)
                j = float(jitter)
            except Exception:
                return 0.0
            if b < 0:
                b = 0
            if j <= 0:
                return b
            try:
                return b + random.uniform(0, j)
            except Exception:
                return b

        def short_retry_wait():
            cooldown_wait(5)

        with Progress(
            TextColumn("{task.description}", markup=False),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}", markup=False),
            TimeElapsedColumn(),
            console=console,
            refresh_per_second=10,
        ) as progress:
            task_id = progress.add_task("Posting", total=len(pending))

            try:
                for idx, post in enumerate(pending, 1):
                    row_ref = post.get("row")
                    progress.update(task_id, description=f"{post['type'].upper()} (row {row_ref})")
                    logger.info(f"\n[{idx}/{len(pending)}] 📝 {post['type'].upper()} (row {row_ref})")
                    logger.info("─" * 50)

                    max_attempts = max(1, int(getattr(Config, "POST_MAX_ATTEMPTS", 1) or 1))
                    attempt = 0
                    row_done = False

                    while attempt < max_attempts and not row_done:
                        attempt += 1
                        try:
                            logger.info(f"Attempt {attempt}/{max_attempts} (row {row_ref})")

                            # Small randomized delay before each attempt to reduce rate-limit patterns
                            pre = _jitter_seconds(Config.POST_PRE_SUBMIT_DELAY_SECONDS, Config.POST_PRE_SUBMIT_JITTER_SECONDS)
                            if pre > 0:
                                cooldown_wait(int(pre))

                            result = None
                            if post["type"] == "text":
                                result = creator.create_text_post(
                                    title=post.get("title", ""),
                                    content=post.get("content", ""),
                                    tags=post.get("tags", "")
                                )
                            elif post["type"] == "image":
                                result = creator.create_image_post(
                                    image_path=post.get("image_path", ""),
                                    title=post.get("title", ""),
                                    content=post.get("content", ""),
                                    tags=post.get("tags", "")
                                )
                            else:
                                logger.error(f"Unknown type: {post['type']}")
                                sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                                sheets_mgr.update_cell(post_queue, post["row"], col_notes, "Invalid type")
                                failed += 1
                                row_done = True
                                break

                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            status = (result.get("status") if result else "") or "Error"
                            result_url = (result.get("url") if result else "") or ""

                            if status == "Rate Limited":
                                sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                                sheets_mgr.update_cell(post_queue, post["row"], col_notes, "Rate limited")
                                failed += 1
                                post_history.record_post(
                                    post_type=post.get("type", ""),
                                    title=post.get("title", ""),
                                    image_path=post.get("image_path", ""),
                                    post_url="",
                                    status="Failed",
                                    notes="Rate limited"
                                )
                                row_done = True
                                break

                            if status == "Repeating":
                                sheets_mgr.update_cell(post_queue, post["row"], col_status, "Repeating")
                                sheets_mgr.update_cell(
                                    post_queue,
                                    post["row"],
                                    col_notes,
                                    f"Image rejected: repeating/duplicate (attempt {attempt}/{max_attempts})"
                                )
                                post_history.record_post(
                                    post_type=post.get("type", ""),
                                    title=post.get("title", ""),
                                    image_path=post.get("image_path", ""),
                                    post_url="",
                                    status="Repeating",
                                    notes="Image rejected: repeating/duplicate"
                                )

                                if attempt < max_attempts:
                                    short_retry_wait()
                                    continue
                                failed += 1
                                row_done = True
                                break

                            if status in {"Denied", "Error"}:
                                sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                                sheets_mgr.update_cell(
                                    post_queue,
                                    post["row"],
                                    col_notes,
                                    f"{status} (attempt {attempt}/{max_attempts}): {result_url}"
                                )
                                post_history.record_post(
                                    post_type=post.get("type", ""),
                                    title=post.get("title", ""),
                                    image_path=post.get("image_path", ""),
                                    post_url=result_url,
                                    status="Failed",
                                    notes=f"{status}: {result_url}"
                                )

                                if attempt < max_attempts:
                                    short_retry_wait()
                                    continue
                                failed += 1
                                row_done = True
                                break

                            if status == "Dry Run" or "Posted" in status:
                                sheets_mgr.update_cell(post_queue, post["row"], col_status, "Done")
                                if result_url:
                                    sheets_mgr.update_cell(post_queue, post["row"], col_post_url, result_url)
                                sheets_mgr.update_cell(post_queue, post["row"], col_timestamp, timestamp)
                                sheets_mgr.update_cell(post_queue, post["row"], col_notes, status)
                                success += 1
                                post_history.record_post(
                                    post_type=post.get("type", ""),
                                    title=post.get("title", ""),
                                    image_path=post.get("image_path", ""),
                                    post_url=result_url,
                                    status=status,
                                    notes=status
                                )
                                try:
                                    activity.log(
                                        mode="post",
                                        action="create_post",
                                        nick="",
                                        url=result_url,
                                        status=status,
                                        details=f"type={post.get('type','')}"
                                    )
                                except Exception:
                                    pass
                                row_done = True
                                if idx < len(pending):
                                    cooldown_wait(123)
                                break

                            if "Verification" in status:
                                is_post_url = ProfileScraper.is_valid_url(result_url)
                                if (not is_post_url) or PostCreator._is_denied_or_share_url(result_url):
                                    sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                                    sheets_mgr.update_cell(post_queue, post["row"], col_notes, f"{status}")
                                    if attempt < max_attempts:
                                        short_retry_wait()
                                        continue
                                    failed += 1
                                    row_done = True
                                else:
                                    sheets_mgr.update_cell(post_queue, post["row"], col_status, "Done")
                                    if result_url:
                                        sheets_mgr.update_cell(post_queue, post["row"], col_post_url, result_url)
                                    sheets_mgr.update_cell(post_queue, post["row"], col_timestamp, timestamp)
                                    sheets_mgr.update_cell(post_queue, post["row"], col_notes, status)
                                    success += 1
                                    row_done = True
                                    if idx < len(pending):
                                        cooldown_wait(123)
                                break

                            sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                            sheets_mgr.update_cell(post_queue, post["row"], col_notes, f"{status}")
                            if attempt < max_attempts:
                                short_retry_wait()
                                continue
                            failed += 1
                            row_done = True
                            break

                        except KeyboardInterrupt:
                            raise
                        except Exception as e:
                            logger.error(f"Error: {e}")
                            try:
                                sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                                sheets_mgr.update_cell(
                                    post_queue,
                                    post["row"],
                                    col_notes,
                                    f"Exception (attempt {attempt}/{max_attempts}): {str(e)[:50]}"
                                )
                            except Exception:
                                pass
                            if attempt < max_attempts:
                                short_retry_wait()
                                continue
                            failed += 1
                            row_done = True
                            break

                    progress.advance(task_id, 1)

            except KeyboardInterrupt:
                logger.warning("\n⚠️ Interrupted by user")

        logger.info("\n" + "=" * 70)
        logger.success(f"✅ Success: {success}/{len(pending)}")
        logger.error(f"❌ Failed: {failed}/{len(pending)}")
        if skipped:
            logger.warning(f"⚠️ Skipped: {skipped}/{len(pending)}")
        if stop_reason:
            logger.warning(f"Stopped early: {stop_reason}")
        logger.info("=" * 70 + "\n")

    finally:
        browser_mgr.close()

# ============================================================================
# PHASE 3: INBOX MODE
# ============================================================================

def run_activity_mode(args):
    """Phase 3a: Fetch DamaDam activity feed → log to MasterLog only. No replies."""
    logger = Logger("activity")
    _print_sheet_context(logger)
    logger.info("=" * 70)
    logger.info(f"DamaDam Bot V{VERSION} - ACTIVITY MODE")
    logger.info("=" * 70 + "\n")

    browser_mgr = BrowserManager(logger)
    driver = browser_mgr.setup()
    if not driver:
        return

    try:
        logger.info("🔐 Login...")
        if not browser_mgr.login():
            logger.error("Login failed")
            return
        logger.success("✅ Login Success\n")

        sheets_mgr = SheetsManager(logger)
        logger.info("📊 Connecting to Google Sheets...")
        if not sheets_mgr.connect():
            logger.error("Sheets connection failed")
            return
        logger.success("✅ Sheet Connected\n")

        activity_log = ActivityLogger(sheets_mgr, logger)
        activity_log.initialize()

        monitor = InboxMonitor(driver, logger)

        logger.info("📊 Fetching Activity Feed...")
        activity_items = monitor.fetch_activity(max_items=60, max_pages=5)

        if not activity_items:
            logger.info("No activity items found.")
            return

        logger.success(f"🧾 Found {len(activity_items)} activity items\n")

        written = 0
        for it in activity_items:
            try:
                activity_log.log(
                    mode="activity",
                    action="activity_feed",
                    nick="",
                    url=(it.get("url") or ""),
                    status="info",
                    details=(it.get("text") or "")[:500]
                )
                written += 1
            except Exception:
                pass

        logger.info("\n" + "=" * 70)
        logger.success(f"✅ Logged {written}/{len(activity_items)} activity items to MasterLog")
        logger.info(f"📞 API Calls: {sheets_mgr.api_calls}")
        logger.info(f"📝 Log: {logger.log_file}")
        logger.info("=" * 70 + "\n")

    finally:
        browser_mgr.close()


def run_inbox_mode(args):
    """Phase 3b: Monitor inbox conversations and send pending replies."""
    logger = Logger("inbox")
    _print_sheet_context(logger)
    logger.info("=" * 70)
    logger.info(f"DamaDam Bot V{VERSION} - INBOX MODE")
    logger.info("=" * 70 + "\n")

    browser_mgr = BrowserManager(logger)
    driver = browser_mgr.setup()
    if not driver:
        return

    try:
        logger.info("🔐 Login...")
        if not browser_mgr.login():
            logger.error("Login failed")
            return
        logger.success("✅ Login Success\n")

        sheets_mgr = SheetsManager(logger)
        logger.info("📊 Connecting to Google Sheets...")
        if not sheets_mgr.connect():
            logger.error("Sheets connection failed")
            return
        logger.success("✅ Sheet Connected\n")

        activity = ActivityLogger(sheets_mgr, logger)
        activity.initialize()
        conv_logger = ConversationLogger(sheets_mgr, logger)
        conv_logger.initialize()

        monitor = InboxMonitor(driver, logger)

        # Resolve InboxQueue sheet (try new name, then legacy names)
        inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "InboxQueue", create_if_missing=False)
        if not inbox_queue:
            inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "Inbox", create_if_missing=False)
        if not inbox_queue:
            inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "Inbox & Activity", create_if_missing=False)
        if not inbox_queue:
            inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "InboxQueue")
        if not inbox_queue:
            logger.error("InboxQueue sheet not found")
            return

        logger.info("📥 Fetching inbox conversations...")
        inbox_messages = monitor.fetch_inbox()
        logger.success(f"Found {len(inbox_messages)} conversations\n")

        sheets_mgr.api_calls += 1
        existing_rows = inbox_queue.get_all_values()
        existing_nicks = {row[0].strip().lower() for row in existing_rows[1:] if row}
        existing_last_msg = {
            (row[0].strip().lower()): (row[2].strip() if len(row) > 2 else "")
            for row in existing_rows[1:]
            if row and row[0].strip()
        }

        # Sync new conversations into InboxQueue
        new_count = 0
        appended_this_run: set = set()
        for msg in inbox_messages:
            nick_key = (msg.get("nick") or "").strip().lower()
            if not nick_key or nick_key in appended_this_run:
                continue
            if nick_key not in existing_nicks:
                values = [
                    msg["nick"], msg["nick"], msg["last_msg"], "",
                    "pending", msg["timestamp"], "", ""
                ]
                sheets_mgr.append_row(inbox_queue, values)
                logger.info(f"➕ New conversation: {msg['nick']}")
                new_count += 1
                appended_this_run.add(nick_key)
                existing_nicks.add(nick_key)
                try:
                    activity.log(
                        mode="inbox", action="new_conversation",
                        nick=msg.get("nick", ""), url=msg.get("conv_url", ""),
                        status="pending", details=(msg.get("last_msg", "") or "")[:500]
                    )
                except Exception:
                    pass

            # Log inbound messages that are new since last run
            try:
                last_now = (msg.get("last_msg") or "").strip()
                last_prev = (existing_last_msg.get(nick_key) or "").strip()
                if last_now and last_now != last_prev:
                    conv_logger.log(
                        nick=msg.get("nick", ""), direction="IN", mode="inbox",
                        message=last_now, url=msg.get("conv_url", ""), status="received"
                    )
                    activity.log(
                        mode="inbox", action="inbound_message",
                        nick=msg.get("nick", ""), url=msg.get("conv_url", ""),
                        status="received", details=last_now[:500]
                    )
            except Exception:
                pass

        if new_count:
            logger.success(f"Synced {new_count} new conversations to InboxQueue\n")

        # Reload sheet to get MY_REPLY values
        sheets_mgr.api_calls += 1
        all_rows = inbox_queue.get_all_values()

        pending_replies = []
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) >= 5 and row[3].strip() and row[4].strip().lower().startswith("pending"):
                pending_replies.append({
                    "row": i,
                    "nick": row[0].strip(),
                    "reply": row[3].strip()
                })

        if not pending_replies:
            logger.info("✅ No pending replies to send.")
            logger.info(f"📝 Log: {logger.log_file}")
            return

        logger.info(f"📤 Sending {len(pending_replies)} replies...\n")

        success = 0
        for idx, reply in enumerate(pending_replies, 1):
            logger.info(f"[{idx}/{len(pending_replies)}] Replying to: {reply['nick']}")
            try:
                conv_url = None
                for msg in inbox_messages:
                    if msg["nick"].lower() == reply["nick"].lower():
                        conv_url = msg["conv_url"]
                        break
                if not conv_url:
                    conv_url = f"{Config.BASE_URL}/inbox/{reply['nick']}/"

                if monitor.send_reply(conv_url, reply["reply"]):
                    conv_log = monitor.get_conversation_log(conv_url)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    sheets_mgr.update_cell(inbox_queue, reply["row"], 5, "sent")
                    sheets_mgr.update_cell(inbox_queue, reply["row"], 6, timestamp)
                    if conv_log:
                        sheets_mgr.update_cell(inbox_queue, reply["row"], 8, conv_log)
                    success += 1
                    logger.success(f"  ✅ Sent to {reply['nick']}")
                    try:
                        activity.log(
                            mode="inbox", action="send_reply",
                            nick=reply.get("nick", ""), url=conv_url,
                            status="sent", details=(reply.get("reply", "") or "")[:500]
                        )
                        conv_logger.log(
                            nick=reply.get("nick", ""), direction="OUT", mode="inbox",
                            message=reply.get("reply", ""), url=conv_url, status="sent"
                        )
                    except Exception:
                        pass
                else:
                    logger.error(f"  ❌ Failed to send to {reply['nick']}")

                time.sleep(2)
            except Exception as e:
                logger.error(f"Reply error for {reply['nick']}: {e}")

        logger.info("\n" + "=" * 70)
        logger.success(f"✅ Sent: {success}/{len(pending_replies)}")
        logger.info(f"📞 API Calls: {sheets_mgr.api_calls}")
        logger.info(f"📝 Log: {logger.log_file}")
        logger.info("=" * 70 + "\n")

    finally:
        browser_mgr.close()

# ============================================================================
# MAIN
# ============================================================================

def main():
    try:
        import logging
        from dotenv import load_dotenv
        logging.getLogger("dotenv").setLevel(logging.ERROR)
        logging.getLogger("dotenv.main").setLevel(logging.ERROR)
        load_dotenv(override=False)
    except Exception:
        pass

    parser = argparse.ArgumentParser(
        description=f"DamaDam Bot V{VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--mode",
        choices=["msg", "populate", "post", "inbox", "activity", "logs", "setup"],
        default=None,
        help="Operation mode"
    )

    parser.add_argument(
        "--max-profiles",
        type=int,
        default=None,
        help="Max targets to process"
    )

    parser.add_argument(
        "--populate-img-links",
        action="store_true",
        help="Populate PostQueue IMG_LINK from POST_LINK when IMG_LINK is empty"
    )

    parser.add_argument(
        "--populate-limit",
        type=int,
        default=None,
        help="Limit Rekhta populate rows (0=all)"
    )

    parser.add_argument(
        "--populate-write",
        action="store_true",
        help="Write Rekhta populate results to PostQueue (disable preview)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without posting/sending and without writing to Google Sheets"
    )

    parser.add_argument(
        "--no-menu",
        action="store_true",
        help="Disable interactive menu and use defaults"
    )

    args = parser.parse_args()

    # Allow CLI flag to override env-configured dry-run at runtime.
    try:
        Config.DRY_RUN = bool(Config.DRY_RUN or getattr(args, "dry_run", False))
    except Exception:
        pass

    def _prompt_int(prompt: str, default: int) -> int:
        try:
            raw = input(prompt).strip()
        except KeyboardInterrupt:
            raise
        except Exception:
            raw = ""
        if not raw:
            return default
        try:
            return int(raw)
        except Exception:
            return default

    def _prompt_choice(prompt: str, choices: Dict[str, str], default_key: str) -> str:
        try:
            raw = input(prompt).strip()
        except KeyboardInterrupt:
            raise
        except Exception:
            raw = ""
        if not raw:
            raw = default_key
        return choices.get(raw, choices.get(default_key))

    # If mode not provided, show interactive menu (unless disabled)
    if not args.no_menu and not args.mode:
        try:
            console.print("\nSelect mode:")
            console.print("  1) Message Bot")
            console.print("  2) Rekhta Mode")
            console.print("  3) Posting Bot")
            console.print("  4) Inbox Mails")
            console.print("  5) Log Reports")
            console.print("  6) Setup Sheets")

            main_map = {
                "1": "msg",
                "2": "populate",
                "3": "post",
                "4": "inbox_menu",
                "5": "logs",
                "6": "setup",
            }
            selected = _prompt_choice("Enter choice [1]: ", main_map, "1")

            if selected == "inbox_menu":
                console.print("\nInbox Mails:")
                console.print("  1) Activity History")
                console.print("  2) Check Inbox")
                inbox_sel = _prompt_choice("Enter choice [2]: ", {"1": "activity", "2": "inbox"}, "2")
                args.mode = inbox_sel
            else:
                args.mode = selected

            if args.mode == "msg":
                max_profiles = _prompt_int("How many profiles? (0=all) [0]: ", 0)
                args.max_profiles = max_profiles
            elif args.mode == "populate":
                max_items = _prompt_int("How many items to populate? (0=all) [0]: ", 0)
                args.populate_limit = max_items
                args.populate_write = True
            elif args.mode == "post":
                max_profiles = _prompt_int("How many posts? (0=all) [0]: ", 0)
                args.max_profiles = max_profiles
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted[/yellow]")
            return

    if args.max_profiles is not None:
        try:
            mp = int(args.max_profiles)
        except Exception:
            mp = 0
        Config.MAX_PROFILES = 0 if mp <= 0 else mp

    if args.populate_limit is not None:
        try:
            pl = int(args.populate_limit)
        except Exception:
            pl = 0
        Config.REKHTA_POPULATE_LIMIT = 0 if pl <= 0 else pl

    try:
        if args.mode == "msg":
            run_message_mode(args)
        elif args.mode == "populate":
            run_populate_mode(args)
        elif args.mode == "post":
            run_post_mode(args)
        elif args.mode == "inbox":
            run_inbox_mode(args)
        elif args.mode == "activity":
            run_activity_mode(args)
        elif args.mode == "logs":
            run_logs_mode()
        elif args.mode == "setup":
            run_setup_mode()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted[/yellow]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
