"""
utils/logger.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Console + file logging with PKT timestamps.
"""

import sys
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from config import Config


# ── Pakistan Standard Time helper ─────────────────────────────────────────────
PKT = timezone(timedelta(hours=5))

def now_pkt() -> datetime:
    """Return current datetime in Pakistan Standard Time (UTC+5)."""
    return datetime.now(tz=PKT)

def pkt_stamp() -> str:
    """Return formatted PKT timestamp string for logs."""
    return now_pkt().strftime("%d-%b-%y %I:%M:%S %p")


# ── Log level icons ────────────────────────────────────────────────────────────
_ICONS = {
    "INFO":    "ℹ️ ",
    "OK":      "✅",
    "WARNING": "⚠️ ",
    "ERROR":   "❌",
    "DEBUG":   "🔍",
    "SKIP":    "⏩",
    "POST":    "📤",
    "MSG":     "💬",
    "REKHTA":  "📜",
}


class Logger:
    """
    Dual-output logger: writes to console AND a daily log file under logs/.

    Usage:
        log = Logger("msg")          # creates logs/msg_YYYY-MM-DD.log
        log.info("Starting...")
        log.ok("Done!")
        log.warning("Watch out")
        log.error("Something failed")
        log.debug("Only shown in DEBUG mode")
    """

    def __init__(self, mode: str = "bot"):
        self.mode = mode.upper()

        # -- Build log file path ------------------------------------------------
        date_str = now_pkt().strftime("%Y-%m-%d")
        log_file = Config.LOG_DIR / f"{mode}_{date_str}.log"

        # -- Configure Python logging to file ----------------------------------
        self._file_logger = logging.getLogger(f"ddbot.{mode}")
        self._file_logger.setLevel(logging.DEBUG)
        self._file_logger.handlers.clear()

        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                           datefmt="%d-%b-%y %I:%M:%S %p"))
        self._file_logger.addHandler(fh)

    def _print(self, level: str, msg: str):
        """Format and print one log line to console + file."""
        icon  = _ICONS.get(level, "  ")
        stamp = pkt_stamp()
        line  = f"{stamp} {icon} [{level}] {msg}"
        print(line, flush=True)
        # Also write to file (without icon to keep file clean)
        file_line = f"{stamp} [{level}] {msg}"
        if level == "DEBUG":
            self._file_logger.debug(file_line)
        elif level in ("WARNING", "SKIP"):
            self._file_logger.warning(file_line)
        elif level == "ERROR":
            self._file_logger.error(file_line)
        else:
            self._file_logger.info(file_line)

    # ── Public log methods ─────────────────────────────────────────────────────

    def info(self, msg: str):
        self._print("INFO", msg)

    def ok(self, msg: str):
        """Success message."""
        self._print("OK", msg)

    def warning(self, msg: str):
        self._print("WARNING", msg)

    def error(self, msg: str):
        self._print("ERROR", msg)

    def skip(self, msg: str):
        """Used when a row is intentionally skipped."""
        self._print("SKIP", msg)

    def debug(self, msg: str):
        """Only shown when Config.DEBUG is True."""
        if Config.DEBUG:
            self._print("DEBUG", msg)

    def section(self, title: str):
        """Print a visual section separator."""
        line = "─" * 60
        print(f"\n{line}", flush=True)
        print(f"  {title}", flush=True)
        print(f"{line}", flush=True)
        self._file_logger.info(f"=== {title} ===")

    def dry_run(self, msg: str):
        """Highlight dry-run actions."""
        self._print("INFO", f"[DRY RUN] {msg}")
