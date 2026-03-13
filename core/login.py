"""
core/login.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DamaDam authentication: cookie login → primary → backup account.
"""

import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import Config
from utils.logger import Logger
from core.browser import save_cookies, load_cookies


# ── DamaDam login page selectors ───────────────────────────────────────────────
_SEL_USERNAME = "#nick, input[name='nick']"
_SEL_PASSWORD = "#pass, input[name='pass'], input[type='password']"
_SEL_SUBMIT   = "button[type='submit'], form button"


class LoginManager:
    """
    Handles DamaDam authentication.

    Flow:
      1. Try cookie login (reuses previous session — fastest, no credentials exposed)
      2. Try primary account fresh login
      3. Try backup account fresh login (if configured)
    """

    def __init__(self, driver, logger: Logger):
        self.driver = driver
        self.log    = logger

    def login(self) -> bool:
        """
        Attempt login using the full fallback chain.
        Returns True if any method succeeds.
        """
        # Skip cookie login in GitHub Actions (no file persistence between runs)
        if not Config.IS_CI:
            if self._try_cookie_login():
                self.log.ok("Logged in via saved session cookies")
                return True
            self.log.info("Cookie login failed — trying fresh login...")

        # Primary account
        if self._fresh_login(Config.DD_NICK, Config.DD_PASS, "Primary"):
            self.log.ok(f"Logged in as primary account: {Config.DD_NICK}")
            return True

        # Backup account (optional)
        if Config.DD_NICK2 and Config.DD_PASS2:
            self.log.warning("Primary login failed — trying backup account...")
            if self._fresh_login(Config.DD_NICK2, Config.DD_PASS2, "Backup"):
                self.log.ok(f"Logged in as backup account: {Config.DD_NICK2}")
                return True

        self.log.error("All login attempts failed")
        return False

    def _try_cookie_login(self) -> bool:
        """
        Navigate to the home page, inject saved cookies, and reload.
        Checks if we end up on a non-login page (= success).
        """
        try:
            self.driver.get(Config.HOME_URL)
            time.sleep(2)
            if not load_cookies(self.driver, self.log):
                return False
            self.driver.refresh()
            time.sleep(3)
            # If still on login page → cookies expired
            return "login" not in self.driver.current_url.lower()
        except Exception as e:
            self.log.debug(f"Cookie login error: {e}")
            return False

    def _fresh_login(self, nick: str, password: str, label: str) -> bool:
        """
        Fill and submit the DamaDam login form with the given credentials.

        Args:
            nick:     DamaDam username
            password: Account password
            label:    'Primary' or 'Backup' (for log messages)
        """
        try:
            self.log.info(f"Attempting {label} login...")
            self.driver.get(Config.LOGIN_URL)
            time.sleep(3)

            # -- Find form fields ----------------------------------------------
            nick_input = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, _SEL_USERNAME))
            )
            try:
                pass_input = self.driver.find_element(By.CSS_SELECTOR, _SEL_PASSWORD)
            except Exception:
                pass_input = WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
                )
            submit_btn = self.driver.find_element(By.CSS_SELECTOR, _SEL_SUBMIT)

            # -- Fill form -----------------------------------------------------
            nick_input.clear()
            nick_input.send_keys(nick)
            time.sleep(0.4)
            pass_input.clear()
            pass_input.send_keys(password)
            time.sleep(0.4)
            submit_btn.click()
            time.sleep(4)

            # -- Check result --------------------------------------------------
            if "login" not in self.driver.current_url.lower():
                # Save cookies for next run (skip in CI)
                if not Config.IS_CI:
                    save_cookies(self.driver, self.log)
                return True

            self.log.warning(f"{label} account login failed (still on login page)")
            return False

        except Exception as e:
            self.log.error(f"{label} login error: {e}")
            return False
