"""
core/browser.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Chrome WebDriver lifecycle management.
Taken from DD-CMS-Final proven working setup, adapted for this project.
"""

import pickle
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from config import Config
from utils.logger import Logger


class BrowserManager:
    """
    Manages Chrome WebDriver startup, configuration, and teardown.

    Usage:
        bm = BrowserManager(logger)
        driver = bm.start()
        if not driver:
            exit(...)
        # ... use driver ...
        bm.close()
    """

    def __init__(self, logger: Logger):
        self.log    = logger
        self.driver = None

    def start(self):
        """
        Initialize and configure Chrome WebDriver.
        Returns the driver instance, or None on failure.
        """
        self.log.info("Initializing Chrome browser...")
        try:
            opts = Options()

            # -- Headless mode -------------------------------------------------
            if Config.HEADLESS:
                opts.add_argument("--headless=new")

            # -- Anti-detection ------------------------------------------------
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)

            # -- Performance options -------------------------------------------
            opts.add_argument("--window-size=1280,800")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--log-level=3")
            opts.add_argument("--disable-infobars")
            opts.add_argument("--disable-notifications")
            opts.add_argument("--disable-popup-blocking")
            opts.add_argument("--mute-audio")
            opts.add_argument("--no-pings")
            opts.add_argument("--disable-extensions")
            opts.add_argument("--disable-sync")
            opts.add_argument("--disable-background-networking")
            opts.add_argument("--no-first-run")
            # Skip image loading — speeds up page loads significantly
            opts.add_argument("--blink-settings=imagesEnabled=false")
            # Eager = don't wait for all resources, just DOM ready
            opts.page_load_strategy = "eager"

            # -- Start driver --------------------------------------------------
            if Config.CHROMEDRIVER_PATH and Path(Config.CHROMEDRIVER_PATH).exists():
                self.log.debug(f"Using custom ChromeDriver: {Config.CHROMEDRIVER_PATH}")
                svc = Service(executable_path=Config.CHROMEDRIVER_PATH)
                self.driver = webdriver.Chrome(service=svc, options=opts)
            else:
                self.log.debug("Using system ChromeDriver (found in PATH)")
                self.driver = webdriver.Chrome(options=opts)

            # -- Post-start configuration --------------------------------------
            self.driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
            # Override navigator.webdriver to prevent detection
            self.driver.execute_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )

            self.log.ok("Browser initialized")
            return self.driver

        except Exception as e:
            self.log.error(f"Browser setup failed: {e}")
            return None

    def close(self):
        """Safely shut down the WebDriver."""
        if self.driver:
            try:
                self.driver.quit()
                self.log.info("Browser closed")
            except Exception:
                pass
            self.driver = None


# ── Cookie persistence ─────────────────────────────────────────────────────────

def save_cookies(driver, logger: Logger = None) -> bool:
    """
    Save current session cookies to a pickle file.
    Allows reuse across runs without re-entering credentials.
    """
    try:
        cookies = driver.get_cookies()
        with open(Config.COOKIE_FILE, "wb") as f:
            pickle.dump(cookies, f)
        if logger:
            logger.debug(f"Session cookies saved ({len(cookies)} items)")
        return True
    except Exception as e:
        if logger:
            logger.warning(f"Cookie save failed: {e}")
        return False


def load_cookies(driver, logger: Logger = None) -> bool:
    """
    Load previously saved cookies into the current browser session.
    Call this AFTER navigating to the target domain (cookies are domain-scoped).
    """
    try:
        cookie_path = Path(Config.COOKIE_FILE)
        if not cookie_path.exists():
            if logger:
                logger.debug("No saved cookies found")
            return False
        with open(cookie_path, "rb") as f:
            cookies = pickle.load(f)
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except Exception:
                pass  # Some cookies may be rejected — that's fine
        if logger:
            logger.debug(f"Cookies loaded ({len(cookies)} items)")
        return True
    except Exception as e:
        if logger:
            logger.warning(f"Cookie load failed: {e}")
        return False
