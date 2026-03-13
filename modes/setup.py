"""
modes/setup.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Setup Mode: Create and format all required sheets in the Google Spreadsheet.
Run this once when setting up a new sheet, or to repair/reset headers.
No browser needed.
"""

from config import Config
from utils.logger import Logger
from core.sheets import SheetsManager


# Map of sheet name → expected column headers
_SHEETS_TO_SETUP = {
    Config.SHEET_MSG_LIST:   Config.MSG_LIST_COLS,
    Config.SHEET_POST_QUEUE: Config.POST_QUEUE_COLS,
    Config.SHEET_MASTER_LOG: Config.MASTER_LOG_COLS,
    Config.SHEET_INBOX:      Config.INBOX_COLS,
}


def run(sheets: SheetsManager, logger: Logger):
    """
    Create all required sheets and ensure headers are correct.
    Existing data rows are never deleted — only the header row is checked/fixed.
    """
    logger.section("SETUP MODE")

    for sheet_name, col_headers in _SHEETS_TO_SETUP.items():
        logger.info(f"Checking: {sheet_name}")

        # Get or create the worksheet
        ws = sheets.get_worksheet(sheet_name, create_if_missing=True,
                                   headers=col_headers)
        if not ws:
            logger.error(f"Could not create/access: {sheet_name}")
            continue

        # Ensure headers match the expected column definitions
        sheets.ensure_headers(ws, col_headers)
        logger.ok(f"{sheet_name} — OK ({len(col_headers)} columns)")

    logger.section("SETUP COMPLETE — All sheets ready")
