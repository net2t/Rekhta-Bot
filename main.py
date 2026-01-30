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

console = Console()
VERSION = "2.0.0"

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

    # Bot Settings
    DEBUG = os.getenv("DD_DEBUG", "0") == "1"
    MAX_PROFILES = int(os.getenv("DD_MAX_PROFILES", "0"))
    MAX_POST_PAGES = int(os.getenv("DD_MAX_POST_PAGES", "4") or "4")
    POST_COOLDOWN_SECONDS = int(os.getenv("DD_POST_COOLDOWN_SECONDS", "120") or "120")
    POST_RETRY_FAILED = os.getenv("DD_POST_RETRY_FAILED", "1") == "1"
    POST_MAX_ATTEMPTS = int(os.getenv("DD_POST_MAX_ATTEMPTS", "3") or "3")

    POST_DENIED_RETRIES = int(os.getenv("DD_POST_DENIED_RETRIES", "1") or "1")
    POST_DENIED_BACKOFF_SECONDS = int(os.getenv("DD_POST_DENIED_BACKOFF_SECONDS", "600") or "600")

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
                if not create_if_missing:
                    return None
                self.logger.warning(f"Sheet '{sheet_name}' not found, creating...")
                return self._create_sheet(workbook, sheet_name)

        except Exception as e:
            self.logger.error(f"Failed to get sheet '{sheet_name}': {e}")
            return None

    def _create_sheet(self, workbook, sheet_name: str):
        """Create new worksheet with appropriate headers"""

        # Define headers for each sheet type
        headers_map = {
            "MsgList": [
                "MODE", "NAME", "NICK/URL", "CITY", "POSTS", "FOLLOWERS", "Gender",
                "MESSAGE", "STATUS", "NOTES", "RESULT URL"
            ],
            "PostQueue": [
                "TYPE", "CONTENT", "IMAGE_PATH",
                "STATUS", "POST_URL", "TIMESTAMP", "NOTES"
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
            "MsgHistory": [
                "TIMESTAMP", "NICK", "NAME", "MESSAGE", "POST_URL",
                "STATUS", "RESULT_URL"
            ],
            "ActivityLog": [
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

    def _format_headers(self, sheet, col_count: int):
        """Freeze header row and apply basic formatting."""
        try:
            sheet.freeze(rows=1)
            header_range = f"A1:{rowcol_to_a1(1, col_count)}"
            sheet.format(
                header_range,
                {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "backgroundColor": {"red": 0.91, "green": 0.94, "blue": 0.98}
                }
            )
        except Exception as e:
            self.logger.debug(f"Header formatting failed: {e}")

    def update_cell(self, sheet, row: int, col: int, value, retries: int = 3):
        """Update cell with retry logic"""
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

    def append_row(self, sheet, values: list, retries: int = 3):
        """Append row with retry logic"""
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
        """Initialize MsgHistory sheet"""
        self.history_sheet = self.sheets.get_sheet(Config.SHEET_ID, "MsgHistory")
        if self.history_sheet:
            self.logger.debug("Message history tracking enabled")
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


class ActivityLogger:
    def __init__(self, sheets_manager: SheetsManager, logger: Logger):
        self.sheets = sheets_manager
        self.logger = logger
        self.sheet = None

    def initialize(self) -> bool:
        self.sheet = self.sheets.get_sheet(Config.SHEET_ID, "ActivityLog")
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

            messages = []

            # Find conversation items
            conversations = self.driver.find_elements(
                By.CSS_SELECTOR,
                "article, .conversation-item, div[class*='inbox'], li"
            )

            if not conversations:
                self.logger.warning("No inbox items found (check page structure)")
                return []

            self.logger.debug(f"Found {len(conversations)} potential inbox items")

            for conv in conversations:
                try:
                    # Extract nickname
                    nick_elem = conv.find_element(
                        By.CSS_SELECTOR,
                        "a[href*='/users/'], b, strong"
                    )
                    nick = nick_elem.text.strip()
                    if not nick:
                        continue

                    # Extract last message preview
                    msg_elem = conv.find_element(
                        By.CSS_SELECTOR,
                        "span, .message-preview, bdi, p"
                    )
                    last_msg = msg_elem.text.strip()

                    # Extract timestamp
                    try:
                        time_elem = conv.find_element(
                            By.CSS_SELECTOR,
                            "time, span.time, .timestamp, small"
                        )
                        timestamp = time_elem.text.strip()
                    except Exception:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # Get conversation URL
                    link_elem = conv.find_element(
                        By.CSS_SELECTOR,
                        "a[href*='/inbox/'], a[href*='/users/']"
                    )
                    conv_url = link_elem.get_attribute("href")

                    messages.append({
                        "nick": nick,
                        "last_msg": last_msg,
                        "timestamp": timestamp,
                        "conv_url": conv_url
                    })

                    self.logger.debug(f"Inbox: {nick} - {last_msg[:30]}...")

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
        try:
            self.logger.debug(f"Opening conversation: {conv_url}")
            self.driver.get(conv_url)
            time.sleep(3)

            # Find reply form
            textarea = self.driver.find_element(
                By.CSS_SELECTOR,
                "textarea[name='message'], textarea"
            )
            send_btn = self.driver.find_element(
                By.CSS_SELECTOR,
                "button[type='submit']"
            )

            # Type and send
            textarea.clear()
            textarea.send_keys(reply_text)
            self.logger.debug(f"Typed reply: {len(reply_text)} chars")
            time.sleep(0.5)

            send_btn.click()
            self.logger.info("Reply sent")
            time.sleep(3)

            # Verify
            self.driver.refresh()
            time.sleep(2)

            if reply_text in self.driver.page_source:
                self.logger.success("Reply verified")
                return True
            else:
                self.logger.warning("Reply sent but not verified")
                return True  # Assume success

        except Exception as e:
            self.logger.error(f"Reply error: {e}")
            return False

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
        logger.info("🔐 Authenticating...")
        if not browser_mgr.login():
            logger.error("Login failed - check credentials")
            return
        logger.success("✅ Login successful\n")

        # Connect to Google Sheets
        logger.info("📊 Connecting to Google Sheets...")
        sheets_mgr = SheetsManager(logger)
        if not sheets_mgr.connect():
            logger.error("Sheets connection failed")
            return
        logger.success("✅ Sheets connected\n")

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
                if "Posted" in result["status"]:
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
# PHASE 2: POST MODE
# ============================================================================

def run_post_mode(args):
    """Phase 2: Create new posts (text/image)"""
    logger = Logger("post")
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
        if not browser_mgr.login():
            return

        sheets_mgr = SheetsManager(logger)
        if not sheets_mgr.connect():
            return

        activity = ActivityLogger(sheets_mgr, logger)
        activity.initialize()

        creator = PostCreator(driver, logger)
        post_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "PostQueue")
        if not post_queue:
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
        for i, row in enumerate(all_rows[1:], start=2):
            if use_headers:
                post_type = get_any(row, "TYPE").lower()
                status = get_any(row, "STATUS", "STATU").lower()

                title_en = get_any(row, "TITLE_EN", "TITLE_ENG", "TITLE EN", "TITLE")
                title_ur = get_any(row, "TITLE_UR", "TITLE_URDU", "TITLE UR", "CAPTION")
                img_link = get_any(row, "IMG_LINK", "IMAGE_PATH", "IMAGE", "IMAGE_URL")

                # User-defined PostQueue rules:
                # - TYPE=text: use column A (TITLE_EN) as content
                # - TYPE=image + STATUS=pending: use column C (IMG_LINK) as image URL and column B (TITLE_UR) as caption
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
            if not should_run and Config.POST_RETRY_FAILED and status.startswith("failed"):
                try:
                    m = re.search(r"attempt\s*(\d+)", (notes_val or "").lower())
                    if m:
                        attempt_num = int(m.group(1)) + 1
                except Exception:
                    attempt_num = 1
                if attempt_num <= max(1, Config.POST_MAX_ATTEMPTS):
                    should_run = True

            if not should_run:
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

        if Config.MAX_PROFILES > 0:
            pending = pending[:Config.MAX_PROFILES]

        logger.success(f"Found {len(pending)} pending posts\n")

        success = 0
        failed = 0

        def cooldown_wait(seconds: int):
            if seconds <= 0:
                return
            try:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("{task.description}"),
                    BarColumn(),
                    TimeElapsedColumn(),
                    console=console,
                ) as cd:
                    t = cd.add_task(f"Cooldown {seconds}s", total=seconds)
                    for _ in range(seconds):
                        time.sleep(1)
                        cd.advance(t, 1)
            except Exception:
                time.sleep(seconds)

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Posting", total=len(pending))

            for idx, post in enumerate(pending, 1):
                title = post.get("title") or "Untitled"
                progress.update(task_id, description=f"{post['type'].upper()}: {title}")
                logger.info(f"\n[{idx}/{len(pending)}] 📝 {post['type'].upper()}: {title}")
                logger.info("─" * 50)

                try:
                    result = None

                    denied_retries = max(0, int(Config.POST_DENIED_RETRIES))
                    for denied_try in range(0, denied_retries + 1):
                        if post["type"] == "text":
                            result = creator.create_text_post(
                                title=post["title"],
                                content=post["content"],
                                tags=post["tags"]
                            )
                        elif post["type"] == "image":
                            result = creator.create_image_post(
                                image_path=post["image_path"],
                                title=post["title"],
                                content=post["content"],
                                tags=post["tags"]
                            )
                        else:
                            logger.error(f"Unknown type: {post['type']}")
                            sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                            sheets_mgr.update_cell(post_queue, post["row"], col_notes, "Invalid type")
                            failed += 1
                            progress.advance(task_id, 1)
                            result = None
                            break

                        if result and (result.get("status") == "Denied"):
                            denied_url = (result.get("url") or "").strip()
                            if PostCreator._is_denied_or_share_url(denied_url) and denied_try < denied_retries:
                                wait_s = max(0, int(Config.POST_DENIED_BACKOFF_SECONDS))
                                if wait_s > 0:
                                    logger.warning(
                                        f"Denied (url={denied_url}). Backing off {wait_s}s then retrying..."
                                    )
                                    cooldown_wait(wait_s)
                                continue
                        break

                    if post["type"] not in {"text", "image"}:
                        continue

                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    attempt_num = int(post.get("attempt") or 1)
                    if result and "Posted" in result["status"]:
                        sheets_mgr.update_cell(post_queue, post["row"], col_status, "Done")
                        sheets_mgr.update_cell(post_queue, post["row"], col_post_url, result["url"])
                        sheets_mgr.update_cell(post_queue, post["row"], col_timestamp, timestamp)
                        sheets_mgr.update_cell(post_queue, post["row"], col_notes, result["status"])
                        success += 1

                        try:
                            activity.log(
                                mode="post",
                                action="create_post",
                                nick="",
                                url=result.get("url", ""),
                                status=result.get("status", ""),
                                details=f"type={post.get('type','')}"
                            )
                        except Exception:
                            pass
                    elif result and "Verification" in result.get("status", ""):
                        result_url = (result.get("url") or "").strip()
                        is_post_url = ProfileScraper.is_valid_url(result_url)
                        if (not is_post_url) or PostCreator._is_denied_or_share_url(result_url):
                            sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                            sheets_mgr.update_cell(
                                post_queue,
                                post["row"],
                                col_notes,
                                f"Attempt {attempt_num}/{Config.POST_MAX_ATTEMPTS} - {result.get('status', 'Error')}"
                            )
                            failed += 1

                            try:
                                activity.log(
                                    mode="post",
                                    action="create_post_failed",
                                    nick="",
                                    url=result_url,
                                    status=result.get("status", "Error"),
                                    details=f"type={post.get('type','')}"
                                )
                            except Exception:
                                pass
                        else:
                            sheets_mgr.update_cell(post_queue, post["row"], col_status, "Done")
                            if result.get("url"):
                                sheets_mgr.update_cell(post_queue, post["row"], col_post_url, result["url"])
                            sheets_mgr.update_cell(post_queue, post["row"], col_timestamp, timestamp)
                            sheets_mgr.update_cell(post_queue, post["row"], col_notes, result["status"])
                            success += 1

                            try:
                                activity.log(
                                    mode="post",
                                    action="create_post",
                                    nick="",
                                    url=result.get("url", ""),
                                    status=result.get("status", ""),
                                    details=f"type={post.get('type','')}"
                                )
                            except Exception:
                                pass
                    else:
                        sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                        sheets_mgr.update_cell(
                            post_queue,
                            post["row"],
                            col_notes,
                            f"Attempt {attempt_num}/{Config.POST_MAX_ATTEMPTS} - {result.get('status', 'Error')}"
                        )
                        failed += 1

                        try:
                            activity.log(
                                mode="post",
                                action="create_post_failed",
                                nick="",
                                url=result.get("url", "") if result else "",
                                status=result.get("status", "Error") if result else "Error",
                                details=f"type={post.get('type','')}"
                            )
                        except Exception:
                            pass

                    time.sleep(3)
                    progress.advance(task_id, 1)

                    if idx < len(pending):
                        s = (result.get("status") if result else "") or ""
                        if s not in {"Image Download Failed", "File Not Found"}:
                            cooldown_wait(Config.POST_COOLDOWN_SECONDS)

                except Exception as e:
                    logger.error(f"Error: {e}")
                    sheets_mgr.update_cell(post_queue, post["row"], col_status, "Failed")
                    sheets_mgr.update_cell(post_queue, post["row"], col_notes, str(e)[:50])
                    failed += 1
                    progress.advance(task_id, 1)

                    try:
                        activity.log(
                            mode="post",
                            action="exception",
                            nick="",
                            url="",
                            status="Exception",
                            details=str(e)[:500]
                        )
                    except Exception:
                        pass

                    if idx < len(pending):
                        cooldown_wait(Config.POST_COOLDOWN_SECONDS)

        logger.info("\n" + "=" * 70)
        logger.success(f"✅ Success: {success}/{len(pending)}")
        logger.error(f"❌ Failed: {failed}/{len(pending)}")
        logger.info("=" * 70 + "\n")

    finally:
        browser_mgr.close()

# ============================================================================
# PHASE 3: INBOX MODE
# ============================================================================

def run_inbox_mode(args):
    """Phase 3: Monitor inbox and send replies"""
    logger = Logger("inbox")
    logger.info("=" * 70)
    logger.info(f"DamaDam Bot V{VERSION} - INBOX MODE")
    logger.info("=" * 70 + "\n")

    browser_mgr = BrowserManager(logger)
    driver = browser_mgr.setup()
    if not driver:
        return

    try:
        if not browser_mgr.login():
            return

        sheets_mgr = SheetsManager(logger)
        if not sheets_mgr.connect():
            return

        activity = ActivityLogger(sheets_mgr, logger)
        activity.initialize()
        conv_logger = ConversationLogger(sheets_mgr, logger)
        conv_logger.initialize()

        monitor = InboxMonitor(driver, logger)

        inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "Inbox", create_if_missing=False)
        if not inbox_queue:
            inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "InboxQueue", create_if_missing=False)
        if not inbox_queue:
            inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "Inbox & Activity", create_if_missing=False)
        if not inbox_queue:
            inbox_queue = sheets_mgr.get_sheet(Config.SHEET_ID, "Inbox")
        if not inbox_queue:
            return

        logger.info("📥 Fetching inbox...")
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

        new_count = 0
        for msg in inbox_messages:
            if msg["nick"].lower() not in existing_nicks:
                values = [
                    msg["nick"], msg["nick"], msg["last_msg"], "",
                    "pending", msg["timestamp"], "", ""
                ]
                sheets_mgr.append_row(inbox_queue, values)
                logger.info(f"➕ New: {msg['nick']}")
                new_count += 1

                try:
                    activity.log(
                        mode="inbox",
                        action="new_conversation",
                        nick=msg.get("nick", ""),
                        url=msg.get("conv_url", ""),
                        status="pending",
                        details=(msg.get("last_msg", "") or "")[:500]
                    )
                except Exception:
                    pass

            try:
                nick_key = (msg.get("nick") or "").strip().lower()
                last_now = (msg.get("last_msg") or "").strip()
                last_prev = (existing_last_msg.get(nick_key) or "").strip()
                if nick_key and last_now and last_now != last_prev:
                    conv_logger.log(
                        nick=msg.get("nick", ""),
                        direction="IN",
                        mode="inbox",
                        message=last_now,
                        url=msg.get("conv_url", ""),
                        status="received"
                    )
                    activity.log(
                        mode="inbox",
                        action="inbound_message",
                        nick=msg.get("nick", ""),
                        url=msg.get("conv_url", ""),
                        status="received",
                        details=last_now[:500]
                    )
            except Exception:
                pass

        if new_count:
            logger.success(f"Added {new_count} new conversations\n")

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
            logger.info("No pending replies")
            return

        logger.info(f"📤 Sending {len(pending_replies)} replies...\n")

        success = 0
        for idx, reply in enumerate(pending_replies, 1):
            logger.info(f"[{idx}/{len(pending_replies)}] {reply['nick']}")

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

                    try:
                        activity.log(
                            mode="inbox",
                            action="send_reply",
                            nick=reply.get("nick", ""),
                            url=conv_url,
                            status="sent",
                            details=(reply.get("reply", "") or "")[:500]
                        )
                        conv_logger.log(
                            nick=reply.get("nick", ""),
                            direction="OUT",
                            mode="inbox",
                            message=reply.get("reply", ""),
                            url=conv_url,
                            status="sent"
                        )
                    except Exception:
                        pass

                time.sleep(2)
            except Exception as e:
                logger.error(f"Error: {e}")

        logger.info("\n" + "=" * 70)
        logger.success(f"✅ Sent: {success}/{len(pending_replies)}")
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
        choices=["msg", "post", "inbox"],
        default="msg",
        help="Operation mode"
    )

    parser.add_argument(
        "--max-profiles",
        type=int,
        default=None,
        help="Max targets to process"
    )

    args = parser.parse_args()

    if args.max_profiles is not None:
        Config.MAX_PROFILES = args.max_profiles

    try:
        if args.mode == "msg":
            run_message_mode(args)
        elif args.mode == "post":
            run_post_mode(args)
        elif args.mode == "inbox":
            run_inbox_mode(args)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted[/yellow]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
