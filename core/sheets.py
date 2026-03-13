"""
core/sheets.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Google Sheets connection and all read/write operations.

Key design decisions:
  - connect() opens the workbook once; all methods reuse the connection
  - get_col(headers, *names) resolves column by name — never by hardcoded number
  - update_cell() / append_row() have built-in retry with exponential backoff
  - batch_read() loads an entire sheet at once for duplicate checking (fast)
"""

import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import gspread
from gspread.exceptions import WorksheetNotFound, APIError
from google.oauth2.service_account import Credentials

from config import Config
from utils.logger import Logger, pkt_stamp


# ── Google Sheets API scope ────────────────────────────────────────────────────
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsManager:
    """
    All Google Sheets operations for DD-Msg-Bot.

    Usage:
        sheets = SheetsManager(logger)
        if not sheets.connect():
            ...exit...
        ws = sheets.get_worksheet("MsgList")
        rows = sheets.read_all(ws)
        sheets.update_cell(ws, row_num, col_num, value)
    """

    def __init__(self, logger: Logger):
        self.log    = logger
        self.client = None
        self._wb    = None   # gspread Spreadsheet object

    # ════════════════════════════════════════════════════════════════════════════
    #  Connection
    # ════════════════════════════════════════════════════════════════════════════

    def connect(self) -> bool:
        """
        Authenticate with Google Sheets API and open the spreadsheet.
        Returns True on success, False on failure.
        """
        try:
            self.log.info("Connecting to Google Sheets API...")

            # -- Build credentials from JSON string or file --------------------
            creds = None
            if Config.CREDENTIALS_JSON:
                # JSON string passed via environment variable
                data = json.loads(Config.CREDENTIALS_JSON)
                pk = data.get("private_key", "")
                if isinstance(pk, str) and "\\n" in pk:
                    data["private_key"] = pk.replace("\\n", "\n")
                creds = Credentials.from_service_account_info(data, scopes=_SCOPES)
                self.log.debug("Using credentials from GOOGLE_CREDENTIALS_JSON env var")
            else:
                # Fall back to credentials.json file
                cred_path = Config.get_credentials_path()
                if not Path(cred_path).exists():
                    self.log.error(f"credentials.json not found at: {cred_path}")
                    return False
                creds = Credentials.from_service_account_file(cred_path, scopes=_SCOPES)
                self.log.debug(f"Using credentials file: {cred_path}")

            self.client = gspread.authorize(creds)
            self._wb    = self.client.open_by_key(Config.SHEET_ID)
            self.log.ok("Google Sheets connected")
            return True

        except Exception as e:
            self.log.error(f"Sheets connection failed: {e}")
            return False

    # ════════════════════════════════════════════════════════════════════════════
    #  Worksheet helpers
    # ════════════════════════════════════════════════════════════════════════════

    def get_worksheet(self, name: str, create_if_missing: bool = True,
                      headers: Optional[List[str]] = None):
        """
        Get a worksheet by name. Optionally create it if it doesn't exist.

        Args:
            name:              Tab name in the spreadsheet
            create_if_missing: If True (default), creates the tab when not found
            headers:           Column headers to write on row 1 when creating

        Returns:
            gspread.Worksheet or None on failure
        """
        try:
            return self._wb.worksheet(name)
        except WorksheetNotFound:
            if not create_if_missing:
                self.log.warning(f"Worksheet '{name}' not found")
                return None
            self.log.info(f"Creating worksheet: {name}")
            return self._create_worksheet(name, headers)
        except Exception as e:
            self.log.error(f"Failed to get worksheet '{name}': {e}")
            return None

    def _create_worksheet(self, name: str, headers: Optional[List[str]] = None):
        """Create a new worksheet and optionally write headers on row 1."""
        try:
            ws = self._wb.add_worksheet(title=name, rows=1000, cols=20)
            if headers:
                ws.update("A1", [headers])
                self._format_header_row(ws, len(headers))
            self.log.ok(f"Created worksheet: {name}")
            return ws
        except Exception as e:
            self.log.error(f"Could not create worksheet '{name}': {e}")
            return None

    def _format_header_row(self, ws, num_cols: int):
        """
        Apply dark background + white bold text to header row (row 1).
        Freezes the first row so it stays visible while scrolling.
        """
        try:
            # Dark background, white bold text, centered
            ws.format("1:1", {
                "backgroundColor":  {"red": 0.1, "green": 0.1, "blue": 0.1},
                "textFormat":       {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER"
            })
            # Freeze the header row
            body = {
                "requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": ws.id,
                            "gridProperties": {"frozenRowCount": 1}
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                }]
            }
            self._wb.batch_update(body)
        except Exception:
            pass  # Formatting is cosmetic — don't fail if it errors

    # ════════════════════════════════════════════════════════════════════════════
    #  Column resolution — ALWAYS use this instead of hardcoded numbers
    # ════════════════════════════════════════════════════════════════════════════

    @staticmethod
    def get_col(headers: List[str], *names: str) -> Optional[int]:
        """
        Find the 1-based column number of a header by trying multiple name variants.

        Args:
            headers: List of header strings from row 1 (0-based internally)
            *names:  One or more column name candidates to try (case-insensitive)

        Returns:
            1-based column number if found, None if not found.

        Example:
            col = SheetsManager.get_col(headers, "NICK", "NICK/URL", "nick")
            # Returns 3 if "NICK" is in column C (index 2 → +1 = 3)
        """
        # Normalize headers once for comparison
        norm = [str(h).strip().upper() for h in headers]
        for name in names:
            key = str(name).strip().upper()
            if key in norm:
                return norm.index(key) + 1  # Convert 0-based to 1-based
        return None

    @staticmethod
    def build_header_map(headers: List[str]) -> Dict[str, int]:
        """
        Build a dict of {UPPER_HEADER_NAME: 0-based-index} for fast cell access.
        Used with get_cell() below.
        """
        return {str(h).strip().upper(): i for i, h in enumerate(headers)}

    @staticmethod
    def get_cell(row: List[str], header_map: Dict[str, int], *names: str) -> str:
        """
        Extract a cell value from a row using header names (case-insensitive).
        Tries each name in order and returns the first non-empty match.

        Args:
            row:        List of cell values (one full sheet row)
            header_map: From build_header_map()
            *names:     Column name candidates

        Returns:
            Cell value as string, or "" if not found.
        """
        for name in names:
            key = str(name).strip().upper()
            if key in header_map:
                idx = header_map[key]
                if 0 <= idx < len(row):
                    val = str(row[idx]).strip()
                    if val:
                        return val
        return ""

    # ════════════════════════════════════════════════════════════════════════════
    #  Read operations
    # ════════════════════════════════════════════════════════════════════════════

    def read_all(self, ws) -> List[List[str]]:
        """
        Read all rows from a worksheet.
        Returns list of rows. Row 0 is headers; data starts at row 1.
        Empty if worksheet is None.
        """
        if not ws:
            return []
        try:
            return ws.get_all_values()
        except Exception as e:
            self.log.error(f"read_all failed on '{ws.title}': {e}")
            return []

    def read_col_values(self, ws, col_num: int) -> List[str]:
        """
        Read a single column as a list of strings (excludes row 1 header).
        Used for fast batch duplicate checking.

        Args:
            ws:      Worksheet
            col_num: 1-based column number

        Returns:
            List of non-empty values in that column (data rows only)
        """
        if not ws:
            return []
        try:
            col_data = ws.col_values(col_num)
            # col_values returns all rows including header; skip first
            return [str(v).strip() for v in col_data[1:] if v]
        except Exception as e:
            self.log.error(f"read_col_values failed: {e}")
            return []

    # ════════════════════════════════════════════════════════════════════════════
    #  Write operations (with retry)
    # ════════════════════════════════════════════════════════════════════════════

    def update_cell(self, ws, row: int, col: int, value: str,
                    retries: int = 3) -> bool:
        """
        Write a single cell value. Retries on API errors with backoff.

        Args:
            ws:    Worksheet
            row:   1-based row number
            col:   1-based column number
            value: Value to write
        """
        if Config.DRY_RUN:
            self.log.dry_run(f"update_cell row={row} col={col} → '{value}'")
            return True
        for attempt in range(1, retries + 1):
            try:
                ws.update_cell(row, col, value)
                return True
            except APIError as e:
                if attempt < retries:
                    wait = 2 ** attempt
                    self.log.warning(f"Sheets API error (attempt {attempt}), retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    self.log.error(f"update_cell failed after {retries} attempts: {e}")
                    return False
            except Exception as e:
                self.log.error(f"update_cell error: {e}")
                return False
        return False

    def update_row_cells(self, ws, row: int, updates: Dict[int, str],
                         retries: int = 3) -> bool:
        """
        Update multiple cells in the same row in one API call (batch update).
        More efficient than calling update_cell() multiple times per row.

        Args:
            ws:      Worksheet
            row:     1-based row number
            updates: Dict of {1-based-col: value}
        """
        if not updates:
            return True
        if Config.DRY_RUN:
            self.log.dry_run(f"update_row_cells row={row} updates={updates}")
            return True
        # Convert to A1 notation for batch_update
        from gspread.utils import rowcol_to_a1
        data = [{"range": rowcol_to_a1(row, col), "values": [[val]]}
                for col, val in updates.items()]
        for attempt in range(1, retries + 1):
            try:
                ws.batch_update(data)
                return True
            except APIError as e:
                if attempt < retries:
                    wait = 2 ** attempt
                    self.log.warning(f"batch update API error, retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    self.log.error(f"update_row_cells failed: {e}")
                    return False
            except Exception as e:
                self.log.error(f"update_row_cells error: {e}")
                return False
        return False

    def append_row(self, ws, values: List, retries: int = 3) -> bool:
        """
        Append a new row at the bottom of a worksheet.

        Args:
            ws:     Worksheet
            values: List of cell values for the new row
        """
        if Config.DRY_RUN:
            self.log.dry_run(f"append_row → {values}")
            return True
        for attempt in range(1, retries + 1):
            try:
                ws.append_row(values, value_input_option="USER_ENTERED")
                return True
            except APIError as e:
                if attempt < retries:
                    wait = 2 ** attempt
                    self.log.warning(f"append_row API error, retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    self.log.error(f"append_row failed: {e}")
                    return False
            except Exception as e:
                self.log.error(f"append_row error: {e}")
                return False
        return False

    # ════════════════════════════════════════════════════════════════════════════
    #  MasterLog
    # ════════════════════════════════════════════════════════════════════════════

    def log_action(self, mode: str, action: str, nick: str = "",
                   url: str = "", status: str = "", details: str = ""):
        """
        Append one row to the MasterLog sheet.
        Called after every significant action across all modes.
        """
        ws = self.get_worksheet(Config.SHEET_MASTER_LOG,
                                 headers=Config.MASTER_LOG_COLS)
        if not ws:
            return
        self.append_row(ws, [
            pkt_stamp(),  # TIMESTAMP
            mode,         # MODE
            action,       # ACTION
            nick,         # NICK
            url,          # URL
            status,       # STATUS
            details,      # DETAILS
        ])

    # ════════════════════════════════════════════════════════════════════════════
    #  Ensure headers are correct (used by Setup Mode)
    # ════════════════════════════════════════════════════════════════════════════

    def ensure_headers(self, ws, expected_cols: List[str]) -> bool:
        """
        Check if row 1 of a worksheet matches expected_cols.
        If headers are missing or wrong, writes the correct headers.

        Args:
            ws:            Worksheet
            expected_cols: List of header names in the correct order

        Returns:
            True if headers are correct (or were fixed), False on error.
        """
        if not ws:
            return False
        try:
            current = ws.row_values(1)
            current_upper = [str(h).strip().upper() for h in current]
            expected_upper = [str(h).strip().upper() for h in expected_cols]
            if current_upper[:len(expected_upper)] != expected_upper:
                self.log.info(f"Updating headers on '{ws.title}'")
                ws.update("A1", [expected_cols])
                self._format_header_row(ws, len(expected_cols))
            return True
        except Exception as e:
            self.log.error(f"ensure_headers failed on '{ws.title}': {e}")
            return False
