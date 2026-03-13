"""
modes/inbox.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Inbox Mode: Monitor DamaDam inbox + send pending replies from InboxQueue sheet.
Activity Mode: Fetch DamaDam activity feed and log to MasterLog.

Both modes are combined into this file since they share the same browser
session and InboxMonitor logic.

What Inbox Mode does:
  1. Open /inbox/ on DamaDam — get list of conversations
  2. Sync any NEW conversations into the InboxQueue sheet
  3. Look for rows in InboxQueue where MY_REPLY has text and STATUS=Pending
  4. Send each reply in the corresponding conversation
  5. Update STATUS to Done

What Activity Mode does:
  1. Open /inbox/activity/ on DamaDam (up to 5 pages)
  2. Parse each activity item (text + URL)
  3. Append to MasterLog
"""

import time
from typing import List, Dict, Optional

from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

from config import Config
from utils.logger import Logger, pkt_stamp
from utils.helpers import strip_non_bmp
from core.sheets import SheetsManager


# ── DamaDam inbox selectors ────────────────────────────────────────────────────
_URL_INBOX    = f"{Config.BASE_URL}/inbox/"
_URL_ACTIVITY = f"{Config.BASE_URL}/inbox/activity/"

# Each inbox conversation appears in a div.mbl.mtl block
_SEL_INBOX_BLOCK    = "div.mbl.mtl"
_SEL_NICK_IN_BLOCK  = "div.cl bdi"
_SEL_MSG_IN_BLOCK   = "div.cl.lsp.nos"
_SEL_TIME_IN_BLOCK  = "span[style*='color:#999'], span.cxs"

# Reply form selectors (same as message mode — DamaDam uses the same form)
_SEL_REPLY_FORM     = "form[action*='/direct-response/send']"
_SEL_REPLY_TEXTAREA = "textarea[name='direct_response']"
_SEL_REPLY_SUBMIT   = "button[type='submit'][name='dec'][value='1'], button[type='submit']"


def run_inbox(driver, sheets: SheetsManager, logger: Logger) -> Dict:
    """
    Run Inbox Mode: sync conversations and send pending replies.

    Args:
        driver:  Selenium WebDriver (already logged in)
        sheets:  Connected SheetsManager
        logger:  Logger

    Returns:
        Stats dict: {new_synced, replies_sent, replies_failed}
    """
    logger.section("INBOX MODE")

    # ── Get InboxQueue sheet ──────────────────────────────────────────────────
    ws = sheets.get_worksheet(Config.SHEET_INBOX, headers=Config.INBOX_COLS)
    if not ws:
        logger.error("InboxQueue sheet not found")
        return {}

    headers    = sheets.read_all(ws)
    if not headers:
        return {}
    col_headers = headers[0]
    header_map  = SheetsManager.build_header_map(col_headers)

    col_status  = sheets.get_col(col_headers, "STATUS")
    col_notes   = sheets.get_col(col_headers, "NOTES")
    col_time    = sheets.get_col(col_headers, "TIMESTAMP")

    def cell(row, *names):
        return SheetsManager.get_cell(row, header_map, *names)

    # ── Step 1: Fetch current inbox from DamaDam ──────────────────────────────
    logger.info("Fetching inbox conversations...")
    inbox_items = _fetch_inbox(driver, logger)
    logger.info(f"Found {len(inbox_items)} conversations in inbox")

    # ── Step 2: Sync new conversations into InboxQueue ────────────────────────
    all_rows   = headers  # already loaded above
    data_rows  = all_rows[1:]

    # Build a set of existing nicks (lowercase) for fast lookup
    existing_nicks = {cell(row, "NICK").lower() for row in data_rows if cell(row, "NICK")}
    new_synced = 0

    for item in inbox_items:
        nick = item["nick"]
        if not nick:
            continue
        if nick.lower() in existing_nicks:
            continue  # Already in sheet
        # Append new conversation row
        row_vals = [
            nick,              # NICK
            nick,              # NAME (default to nick)
            item["last_msg"],  # LAST_MSG
            "",                # MY_REPLY (you fill this in)
            "Pending",         # STATUS
            item["timestamp"], # TIMESTAMP
            "",                # NOTES
        ]
        if sheets.append_row(ws, row_vals):
            logger.ok(f"New conversation synced: {nick}")
            sheets.log_action("INBOX", "new_conversation", nick, "", "Pending", item["last_msg"][:80])
            existing_nicks.add(nick.lower())
            new_synced += 1

    # ── Step 3: Reload sheet and find pending replies ─────────────────────────
    all_rows   = sheets.read_all(ws)
    header_map = SheetsManager.build_header_map(all_rows[0]) if all_rows else {}

    pending_replies: List[Dict] = []
    for i, row in enumerate(all_rows[1:], start=2):
        nick    = cell(row, "NICK")
        reply   = cell(row, "MY_REPLY")
        status  = cell(row, "STATUS").lower()
        if nick and reply and status.startswith("pending"):
            pending_replies.append({"row": i, "nick": nick, "reply": reply})

    if not pending_replies:
        logger.info("No pending replies to send")
        return {"new_synced": new_synced, "replies_sent": 0, "replies_failed": 0}

    logger.info(f"Found {len(pending_replies)} pending replies to send")

    # ── Step 4: Send replies ──────────────────────────────────────────────────
    # Build a nick → conv_url map from the fetched inbox
    nick_to_url = {item["nick"].lower(): item["conv_url"] for item in inbox_items}

    sent   = 0
    failed = 0

    for idx, item in enumerate(pending_replies, start=1):
        nick   = item["nick"]
        reply  = item["reply"]
        row_n  = item["row"]
        logger.info(f"[{idx}/{len(pending_replies)}] Replying to: {nick}")

        # Find conversation URL
        conv_url = nick_to_url.get(nick.lower())
        if not conv_url:
            # Fallback: construct the inbox URL directly
            conv_url = f"{_URL_INBOX}{nick}/"

        ok = _send_reply(driver, conv_url, reply, logger)
        if ok:
            logger.ok(f"Reply sent to {nick}")
            sheets.update_row_cells(ws, row_n, {
                col_status: "Done",
                col_notes:  f"Replied @ {pkt_stamp()}",
            })
            sheets.log_action("INBOX", "reply_sent", nick, conv_url, "Done", reply[:80])
            sent += 1
        else:
            logger.warning(f"Reply failed for {nick}")
            sheets.update_row_cells(ws, row_n, {
                col_status: "Failed",
                col_notes:  f"Send failed @ {pkt_stamp()}",
            })
            sheets.log_action("INBOX", "reply_failed", nick, conv_url, "Failed")
            failed += 1

        time.sleep(2)

    logger.section(
        f"INBOX MODE DONE — New:{new_synced}  Sent:{sent}  Failed:{failed}"
    )
    return {"new_synced": new_synced, "replies_sent": sent, "replies_failed": failed}


def run_activity(driver, sheets: SheetsManager, logger: Logger) -> Dict:
    """
    Run Activity Mode: fetch DamaDam activity feed and log to MasterLog.

    Args:
        driver:  Selenium WebDriver (already logged in)
        sheets:  Connected SheetsManager
        logger:  Logger

    Returns:
        Stats dict: {logged}
    """
    logger.section("ACTIVITY MODE")
    logger.info("Fetching DamaDam activity feed...")

    items  = _fetch_activity(driver, logger, max_items=60, max_pages=5)
    logged = 0

    if not items:
        logger.info("No activity items found")
        return {"logged": 0}

    logger.info(f"Found {len(items)} activity items — writing to MasterLog...")

    for item in items:
        sheets.log_action(
            mode    = "activity",
            action  = "activity_feed",
            nick    = "",
            url     = item.get("url", ""),
            status  = "info",
            details = item.get("text", "")[:500],
        )
        logged += 1
        time.sleep(0.3)  # Gentle rate limit for Sheets API

    logger.ok(f"Logged {logged} activity items to MasterLog")
    return {"logged": logged}


# ════════════════════════════════════════════════════════════════════════════════
#  FETCH INBOX
#  Opens /inbox/ and reads all visible conversation blocks.
# ════════════════════════════════════════════════════════════════════════════════

def _fetch_inbox(driver, logger: Logger) -> List[Dict]:
    """
    Navigate to DamaDam inbox and extract all conversation summaries.

    Returns list of dicts: {nick, last_msg, timestamp, conv_url}
    """
    try:
        driver.get(_URL_INBOX)
        time.sleep(3)

        messages: List[Dict] = []
        seen_nicks: set       = set()

        # Each conversation = a div.mbl.mtl block
        blocks = driver.find_elements(By.CSS_SELECTOR, _SEL_INBOX_BLOCK)
        if not blocks:
            logger.warning("No inbox blocks found — inbox may be empty or selector changed")
            return []

        for block in blocks[:30]:  # cap at 30 most recent conversations
            try:
                # -- Extract nick ----------------------------------------------
                nick = ""
                nick_els = block.find_elements(By.CSS_SELECTOR, _SEL_NICK_IN_BLOCK)
                if nick_els:
                    nick = (nick_els[0].text or "").strip()
                if not nick:
                    continue
                if nick.lower() in seen_nicks:
                    continue
                seen_nicks.add(nick.lower())

                # -- Extract last message text ---------------------------------
                last_msg  = ""
                timestamp = pkt_stamp()
                msg_els = block.find_elements(By.CSS_SELECTOR, _SEL_MSG_IN_BLOCK)
                if msg_els:
                    last_msg = (msg_els[0].text or "").strip()
                    # Timestamp is usually in a small span inside the message line
                    time_els = msg_els[0].find_elements(By.CSS_SELECTOR, _SEL_TIME_IN_BLOCK)
                    if time_els:
                        timestamp = (time_els[-1].text or "").strip() or timestamp

                # -- Extract conversation URL ----------------------------------
                conv_url = _URL_INBOX
                try:
                    links = block.find_elements(
                        By.CSS_SELECTOR,
                        "a[href*='/comments/'], a[href*='/content/'], a[href*='/inbox/']"
                    )
                    for a in links:
                        href = (a.get_attribute("href") or "").strip()
                        if href and href != _URL_INBOX:
                            conv_url = href if href.startswith("http") else f"{Config.BASE_URL}{href}"
                            break
                except Exception:
                    pass

                messages.append({
                    "nick":      nick,
                    "last_msg":  last_msg,
                    "timestamp": timestamp,
                    "conv_url":  conv_url,
                })

            except Exception as e:
                logger.debug(f"Skipped inbox block: {e}")
                continue

        return messages

    except Exception as e:
        logger.error(f"Inbox fetch error: {e}")
        return []


# ════════════════════════════════════════════════════════════════════════════════
#  SEND REPLY
#  Opens a conversation and submits a reply in the direct-response form.
# ════════════════════════════════════════════════════════════════════════════════

def _send_reply(driver, conv_url: str, reply_text: str,
                logger: Logger) -> bool:
    """
    Navigate to conv_url and send reply_text in the reply form.
    Returns True on success, False on failure.
    """
    try:
        driver.get(conv_url)
        time.sleep(3)

        # Find the reply form
        form     = None
        textarea = None

        forms = driver.find_elements(By.CSS_SELECTOR, _SEL_REPLY_FORM)
        for f in forms:
            try:
                ta = f.find_element(By.CSS_SELECTOR, _SEL_REPLY_TEXTAREA)
                form     = f
                textarea = ta
                break
            except Exception:
                continue

        if not form or not textarea:
            # Fallback: try the plain inbox page
            driver.get(_URL_INBOX)
            time.sleep(3)
            forms = driver.find_elements(By.CSS_SELECTOR, _SEL_REPLY_FORM)
            for f in forms:
                try:
                    ta = f.find_element(By.CSS_SELECTOR, _SEL_REPLY_TEXTAREA)
                    form     = f
                    textarea = ta
                    break
                except Exception:
                    continue

        if not form or not textarea:
            logger.warning(f"Reply form not found at {conv_url}")
            return False

        # Find submit button — prefer the "send" variant (dec=1)
        submit_btn = None
        try:
            btns = form.find_elements(
                By.CSS_SELECTOR,
                "button[type='submit'][name='dec'][value='1']"
            )
            if btns:
                submit_btn = btns[0]
        except Exception:
            pass
        if not submit_btn:
            submit_btn = form.find_element(By.CSS_SELECTOR, "button[type='submit']")

        # Type and submit
        safe_reply = strip_non_bmp(reply_text)
        if len(safe_reply) > 350:
            safe_reply = safe_reply[:350]

        textarea.clear()
        time.sleep(0.3)
        textarea.send_keys(safe_reply)
        time.sleep(0.5)

        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(3)

        # Basic verification: check if our reply text appears in page
        try:
            if safe_reply[:30].lower() in driver.page_source.lower():
                logger.debug("Reply verified on page")
                return True
        except Exception:
            pass

        # If we can't verify, assume success (DamaDam sometimes redirects)
        logger.debug("Reply sent (could not verify on page)")
        return True

    except Exception as e:
        logger.error(f"Send reply error: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════════
#  FETCH ACTIVITY
#  Opens /inbox/activity/ and collects activity items across pages.
# ════════════════════════════════════════════════════════════════════════════════

def _fetch_activity(driver, logger: Logger,
                    max_items: int = 60,
                    max_pages: int = 5) -> List[Dict]:
    """
    Fetch DamaDam activity feed items.
    Returns list of dicts: {text, url}
    """
    items: List[Dict] = []
    seen:  set         = set()

    try:
        for page_num in range(1, max_pages + 1):
            if len(items) >= max_items:
                break

            url = _URL_ACTIVITY if page_num == 1 else f"{_URL_ACTIVITY}?page={page_num}"
            driver.get(url)
            time.sleep(3)

            blocks = driver.find_elements(By.CSS_SELECTOR, _SEL_INBOX_BLOCK)
            if not blocks:
                break

            for block in blocks:
                if len(items) >= max_items:
                    break
                try:
                    raw = (block.text or "").strip()
                    if not raw:
                        continue

                    # Clean up navigation noise from the text
                    lines = [
                        ln.strip() for ln in raw.splitlines()
                        if ln.strip() and ln.strip() not in {"►", "REMOVE", "▶"}
                    ]
                    text = "\n".join(lines).strip()
                    if not text:
                        continue

                    # Extract associated URL if present
                    item_url = ""
                    try:
                        links = block.find_elements(
                            By.CSS_SELECTOR,
                            "a[href*='/comments/'], a[href*='/content/']"
                        )
                        if links:
                            href = (links[0].get_attribute("href") or "").strip()
                            if href:
                                item_url = href if href.startswith("http") else f"{Config.BASE_URL}{href}"
                    except Exception:
                        pass

                    # Deduplicate
                    key = (text[:100], item_url)
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append({"text": text[:500], "url": item_url})

                except Exception:
                    continue

            # Check for a next page link
            try:
                next_btns = driver.find_elements(By.CSS_SELECTOR, "a[href*='?page='] button")
                has_next  = any(
                    "NEXT" in (btn.text or "").upper()
                    for btn in next_btns
                )
                if not has_next:
                    break
            except Exception:
                break

    except Exception as e:
        logger.error(f"Activity fetch error: {e}")

    return items
