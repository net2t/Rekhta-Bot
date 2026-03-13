"""
main.py — DD-Msg-Bot V2
Entry point for all bot modes.

Usage:
    python main.py msg       → Message Mode
    python main.py post      → Post Mode
    python main.py rekhta    → Rekhta (Populate PostQueue) Mode
    python main.py inbox     → Inbox Mode (sync conversations + send replies)
    python main.py activity  → Activity Mode (log activity feed)
    python main.py logs      → Show recent MasterLog entries
    python main.py setup     → Create/repair all sheets

Options:
    --max N    Process only N items (0 = unlimited)
    --debug    Verbose debug logging
"""

import sys
import argparse

from config import Config
from utils.logger import Logger
from core.browser import BrowserManager
from core.login import LoginManager
from core.sheets import SheetsManager

import modes.message  as message_mode
import modes.post     as post_mode
import modes.rekhta   as rekhta_mode
import modes.inbox    as inbox_mode
import modes.logs     as logs_mode
import modes.setup    as setup_mode


def _build_parser():
    p = argparse.ArgumentParser(
        prog="main.py",
        description=f"DD-Msg-Bot V{Config.VERSION} — DamaDam automation bot",
    )
    p.add_argument(
        "mode",
        choices=["msg", "post", "rekhta", "inbox", "activity", "logs", "setup"],
        help="Which mode to run",
    )
    p.add_argument(
        "--max", dest="max_items", type=int, default=0, metavar="N",
        help="Maximum items to process (0 = unlimited)",
    )
    p.add_argument("--debug", dest="debug", action="store_true",
                   help="Verbose debug logging")
    p.add_argument("--headless", dest="headless", action="store_true", default=None,
                   help="Force headless browser")
    return p


def _run_with_browser(mode: str, args) -> None:
    """Runner for modes that need a browser."""
    logger = Logger(mode)
    logger.section(f"DD-Msg-Bot V{Config.VERSION} — {mode.upper()} MODE")
    Config.validate()

    bm = BrowserManager(logger)
    driver = bm.start()
    if not driver:
        logger.error("Browser failed to start — aborting")
        sys.exit(1)

    try:
        # Rekhta is a public site — no DamaDam login needed
        if mode != "rekhta":
            lm = LoginManager(driver, logger)
            if not lm.login():
                logger.error("Login failed — aborting")
                sys.exit(1)

        sheets = SheetsManager(logger)
        if not sheets.connect():
            logger.error("Google Sheets connection failed — aborting")
            sys.exit(1)

        max_n = args.max_items

        if mode == "msg":
            message_mode.run(driver, sheets, logger, max_targets=max_n)
        elif mode == "post":
            post_mode.run(driver, sheets, logger, max_posts=max_n)
        elif mode == "rekhta":
            rekhta_mode.run(driver, sheets, logger, max_items=max_n)
        elif mode == "inbox":
            inbox_mode.run_inbox(driver, sheets, logger)
        elif mode == "activity":
            inbox_mode.run_activity(driver, sheets, logger)

    finally:
        bm.close()


def _run_sheets_only(mode: str, args) -> None:
    """Runner for modes that only need Sheets (no browser)."""
    logger = Logger(mode)
    Config.validate()

    sheets = SheetsManager(logger)
    if not sheets.connect():
        logger.error("Google Sheets connection failed")
        sys.exit(1)

    if mode == "logs":
        logs_mode.run(sheets, logger, last_n=30)
    elif mode == "setup":
        setup_mode.run(sheets, logger)


def main():
    parser = _build_parser()
    args   = parser.parse_args()

    if args.debug:
        Config.DEBUG = True
    if args.headless:
        Config.HEADLESS = True

    mode = args.mode
    if mode in ("msg", "post", "rekhta", "inbox", "activity"):
        _run_with_browser(mode, args)
    else:
        _run_sheets_only(mode, args)


if __name__ == "__main__":
    main()
