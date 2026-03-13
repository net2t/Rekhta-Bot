"""
modes/logs.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Logs Mode: Display the most recent rows from MasterLog sheet.
No browser needed — reads sheet only.
"""

from config import Config
from utils.logger import Logger
from core.sheets import SheetsManager


def run(sheets: SheetsManager, logger: Logger, last_n: int = 20):
    """
    Print the last N rows of MasterLog to console.

    Args:
        sheets:  Connected SheetsManager
        logger:  Logger
        last_n:  How many recent rows to show (default: 20)
    """
    logger.section(f"LOGS MODE — last {last_n} entries")

    ws = sheets.get_worksheet(Config.SHEET_MASTER_LOG,
                               create_if_missing=False)
    if not ws:
        logger.warning("MasterLog sheet not found — no logs yet")
        return

    all_rows = sheets.read_all(ws)
    if len(all_rows) < 2:
        logger.info("MasterLog is empty")
        return

    headers  = all_rows[0]
    data     = all_rows[1:]
    recent   = data[-last_n:]  # Last N rows

    # Print a simple table
    col_widths = [18, 6, 8, 16, 40, 8, 30]
    sep        = " | "

    # Header line
    head_parts = []
    for i, h in enumerate(headers[:7]):
        w = col_widths[i] if i < len(col_widths) else 15
        head_parts.append(str(h).upper().ljust(w)[:w])
    print("\n" + sep.join(head_parts))
    print("-" * (sum(col_widths[:7]) + len(sep) * 6))

    for row in recent:
        parts = []
        for i in range(7):
            val = row[i] if i < len(row) else ""
            w   = col_widths[i] if i < len(col_widths) else 15
            parts.append(str(val).ljust(w)[:w])
        print(sep.join(parts))

    print(f"\n({len(data)} total log entries, showing last {len(recent)})\n")
