"""
modes/setup.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Setup Mode  : Delete old sheets and create the full new structure.
Format Mode : Apply Lexend font + dark header styling to all sheets.

Sheet structure created:
  Queue sheets  → MsgQue, PostQue, InboxQue
  Log sheets    → MsgLog, PostLog, InboxLog
  Master log    → Logs
  Dashboard     → Dashboard (empty, formula-based — you fill it)

No browser needed for either mode.
"""

from config import Config
from utils.logger import Logger
from core.sheets import SheetsManager


# ── Old sheet names to delete on fresh setup ──────────────────────────────────
# These are the names from V1 / old structure that should be removed.
_OLD_SHEET_NAMES = [
    "MsgList",
    "PostQueue",
    "InboxQueue",
    "MasterLog",
    "Sheet1",       # Default Google Sheets tab
]


def run(sheets: SheetsManager, logger: Logger):
    """
    Fresh setup:
      1. Delete all old/legacy sheet tabs
      2. Create all new sheet tabs with correct headers
      3. Create empty Dashboard tab
    Existing data will be lost — this is intentional (fresh structure).
    """
    logger.section("SETUP MODE — Fresh Structure")

    # ── Step 1: Delete old sheets ─────────────────────────────────────────────
    logger.info("Removing old sheets...")
    existing = []
    try:
        existing = [ws.title for ws in sheets._wb.worksheets()]
    except Exception as e:
        logger.warning(f"Could not list worksheets: {e}")

    for old_name in _OLD_SHEET_NAMES:
        if old_name in existing:
            try:
                ws = sheets._wb.worksheet(old_name)
                sheets._wb.del_worksheet(ws)
                logger.ok(f"Deleted: {old_name}")
            except Exception as e:
                logger.warning(f"Could not delete '{old_name}': {e}")

    # ── Step 2: Create all new sheets ────────────────────────────────────────
    logger.info("Creating new sheets...")
    for sheet_name, col_headers in Config.ALL_SHEETS.items():
        logger.info(f"Creating: {sheet_name}")
        ws = sheets.get_worksheet(sheet_name, create_if_missing=True,
                                  headers=col_headers)
        if ws:
            sheets.ensure_headers(ws, col_headers)
            logger.ok(f"{sheet_name} — ready ({len(col_headers)} columns)")
        else:
            logger.error(f"Failed to create: {sheet_name}")

    # ── Step 3: Create Dashboard tab (empty — formula-based) ─────────────────
    logger.info("Creating: Dashboard")
    try:
        existing_now = [ws.title for ws in sheets._wb.worksheets()]
        if Config.SHEET_DASHBOARD not in existing_now:
            sheets._wb.add_worksheet(
                title=Config.SHEET_DASHBOARD, rows=50, cols=10
            )
            logger.ok("Dashboard — created (empty, add your formulas here)")
        else:
            logger.ok("Dashboard — already exists")
    except Exception as e:
        logger.warning(f"Dashboard creation failed: {e}")

    logger.section("SETUP COMPLETE — All 8 sheets ready")
    logger.info("Sheet order: MsgQue → PostQue → InboxQue → MsgLog → PostLog → InboxLog → Logs → Dashboard")


def run_format(sheets: SheetsManager, logger: Logger):
    """
    Apply consistent visual formatting to all queue + log sheets:
      - Font:             Lexend, size 10
      - Horizontal align: CENTER for all cells
      - Vertical align:   MIDDLE for all cells
      - Text wrapping:    CLIP (no overflow, no wrap)
      - Header row 1:     Dark background #263238, white bold text, frozen

    Uses Google Sheets API v4 batchUpdate — all formatting in one call per sheet.
    Dashboard is skipped (user-managed).
    """
    logger.section("FORMAT MODE")

    from googleapiclient.discovery import build  # type: ignore

    # ── Build Sheets API v4 service from gspread credentials ─────────────────
    # In gspread 6.x, credentials live at gc.http_client.auth
    try:
        gc      = sheets.client
        creds   = gc.http_client.auth
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as e:
        logger.error(f"Could not build Sheets API service: {e}")
        return

    spreadsheet_id = Config.SHEET_ID

    # ── Colour constants ──────────────────────────────────────────────────────
    # Header background: dark blue-grey #263238
    HEADER_BG   = {"red": 0.149, "green": 0.196, "blue": 0.220}
    HEADER_TEXT = {"red": 1.0,   "green": 1.0,   "blue": 1.0}

    # Format all sheets except Dashboard
    sheets_to_format = {
        k: v for k, v in Config.ALL_SHEETS.items()
        if k != Config.SHEET_DASHBOARD
    }

    for sheet_name, col_headers in sheets_to_format.items():
        logger.info(f"Formatting: {sheet_name}")

        # Get numeric sheetId
        try:
            ws       = sheets.get_worksheet(sheet_name, create_if_missing=False)
            if not ws:
                logger.warning(f"Sheet not found: {sheet_name} — run Setup first")
                continue
            sheet_id = ws.id
        except Exception as e:
            logger.warning(f"Cannot access {sheet_name}: {e}")
            continue

        col_count = len(col_headers)

        requests = [
            # 1. Font + alignment for ALL cells (entire sheet)
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id},
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat":          {"fontFamily": "Lexend", "fontSize": 10},
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment":   "MIDDLE",
                            "wrapStrategy":        "CLIP",
                        }
                    },
                    "fields": (
                        "userEnteredFormat(textFormat,horizontalAlignment,"
                        "verticalAlignment,wrapStrategy)"
                    ),
                }
            },
            # 2. Header row: dark bg, white bold text
            {
                "repeatCell": {
                    "range": {
                        "sheetId":          sheet_id,
                        "startRowIndex":    0,
                        "endRowIndex":      1,
                        "startColumnIndex": 0,
                        "endColumnIndex":   col_count,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": HEADER_BG,
                            "textFormat": {
                                "fontFamily":      "Lexend",
                                "fontSize":        10,
                                "bold":            True,
                                "foregroundColor": HEADER_TEXT,
                            },
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment":   "MIDDLE",
                            "wrapStrategy":        "CLIP",
                        }
                    },
                    "fields": (
                        "userEnteredFormat(backgroundColor,textFormat,"
                        "horizontalAlignment,verticalAlignment,wrapStrategy)"
                    ),
                }
            },
            # 3. Freeze header row
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId":       sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
        ]

        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()
            logger.ok(f"{sheet_name} — formatted")
        except Exception as e:
            logger.error(f"{sheet_name} format failed: {e}")

    logger.section("FORMAT COMPLETE — All sheets styled with Lexend font")
