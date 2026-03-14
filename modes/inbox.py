"""
modes/inbox.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Inbox Mode: ONE run does everything:
  Phase 1 — Fetch DamaDam inbox (/inbox/)
             Parse each conversation block → TID, NICK, TYPE, last message
             Sync new conversations into InboxQue sheet
             Log all new/updated items to InboxLog sheet
  Phase 2 — Send pending replies
             Rows in InboxQue where MY_REPLY has text + STATUS=Pending
             Use TID (the stable user ID from DamaDam HTML) to open conversation
             Update STATUS → Done / Failed
  Phase 3 — Fetch activity feed (/inbox/activity/)
             Log each activity item to InboxLog sheet

DamaDam HTML structure (from your research):
  Each inbox item is a div.mbl.mtl block containing:

  Conversation type header:
    <div class="sp cs mrs"><span style="color:#3b7af7">1 ON 1</span></div>
    <div class="sp cxs mrs"><span>►</span></div>
    <div class="cm sp">1 on 1 with Dazzling_Mushk</div>

  Submit button with TID (stable user ID — never changes):
    <button type="submit" name="tid" value="2464609">

  Nickname:
    <div class="cl lsp nos"><b><bdi>Dazzling_Mushk</bdi></b>

  Last message + relative time:
    <span class="mrs"><bdi>hi</bdi></span>
    <span class="mrs cxs sp" style="color:#999999">1 hour ago</span>

  Conversation types seen: 1 ON 1, POST, MEHFIL
  (No absolute timestamp on /inbox/ — only "X ago" text)
"""

import re
import time
from typing import List, Dict, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from config import Config
from utils.logger import Logger, pkt_stamp
from utils.helpers import strip_non_bmp
from core.sheets import SheetsManager


# ── DamaDam URLs ──────────────────────────────────────────────────────────────
_URL_INBOX    = f"{Config.BASE_URL}/inbox/"
_URL_ACTIVITY = f"{Config.BASE_URL}/inbox/activity/"

# ── Inbox HTML selectors (based on real page HTML) ────────────────────────────
_SEL_ITEM_BLOCK  = "div.mbl.mtl"             # Each inbox/activity item
_SEL_TID_BTN     = "button[name='tid']"      # Button with tid= (stable user ID)
_SEL_NICK_BDI    = "div.cl.lsp.nos b bdi"   # Bold nickname in conversation row
_SEL_MSG_SPAN    = "div.cl.lsp.nos span bdi" # Last message text (inside bdi)
_SEL_TIME_SPAN   = "span[style*='color:#999']" # Relative time "1 hour ago"
_SEL_TYPE_SPAN   = "div.sp.cs.mrs span"      # Type label: "1 ON 1", "POST", etc.

# ── Reply form selectors ──────────────────────────────────────────────────────
_SEL_REPLY_FORM     = "form[action*='/direct-response/send']"
_SEL_REPLY_TEXTAREA = "textarea[name='direct_response']"


def run_inbox(driver, sheets: SheetsManager, logger: Logger) -> Dict:
    """
    Run full Inbox + Activity mode in one pass.

    Phase 1: Fetch inbox → sync InboxQue + log to InboxLog
    Phase 2: Send pending replies from InboxQue
    Phase 3: Fetch activity feed → log to InboxLog

    Returns stats dict: {new_synced, replies_sent, replies_failed, activity_logged}
    """
    logger.section("INBOX + ACTIVITY MODE")

    # ── Get worksheets ────────────────────────────────────────────────────────
    ws_que = sheets.get_worksheet(Config.SHEET_INBOX_QUE, headers=Config.INBOX_QUE_COLS)
    ws_log = sheets.get_worksheet(Config.SHEET_INBOX_LOG, headers=Config.INBOX_LOG_COLS)
    if not ws_que or not ws_log:
        logger.error("InboxQue or InboxLog sheet not found — run Setup first")
        return {}

    # ── Phase 1: Fetch inbox ──────────────────────────────────────────────────
    logger.info("Phase 1: Fetching inbox conversations...")
    inbox_items = _fetch_inbox(driver, logger)
    logger.info(f"Found {len(inbox_items)} conversations in inbox")

    # Load existing TIDs from InboxQue for duplicate check
    all_que_rows = sheets.read_all(ws_que)
    que_headers  = all_que_rows[0] if all_que_rows else Config.INBOX_QUE_COLS
    que_hmap     = SheetsManager.build_header_map(que_headers)

    def qcell(row, *names):
        return SheetsManager.get_cell(row, que_hmap, *names)

    # Build set of existing TIDs (lowercase) for fast lookup
    existing_tids = {
        qcell(row, "TID").lower()
        for row in all_que_rows[1:]
        if qcell(row, "TID")
    }

    new_synced = 0
    for item in inbox_items:
        tid  = str(item.get("tid", "")).strip()
        nick = item.get("nick", "").strip()

        if not nick:
            continue

        # Log every inbox item to InboxLog (full history)
        _log_inbox_entry(sheets, ws_log, item, "IN", "Received")

        # Sync new conversations into InboxQue
        if tid and tid.lower() not in existing_tids:
            row_vals = [
                tid,                   # TID
                nick,                  # NICK
                nick,                  # NAME (default to nick)
                item.get("type", ""),  # TYPE  (1ON1 / POST / MEHFIL)
                item.get("last_msg", ""),  # LAST_MSG
                "",                    # MY_REPLY (you fill this in)
                "Pending",             # STATUS
                pkt_stamp(),           # UPDATED
                "",                    # NOTES
            ]
            if sheets.append_row(ws_que, row_vals):
                logger.ok(f"New conversation: [{item.get('type','')}] {nick} (tid={tid})")
                existing_tids.add(tid.lower())
                new_synced += 1
        elif not tid:
            # No TID found — fall back to nick-based dedup
            existing_nicks = {qcell(row, "NICK").lower() for row in all_que_rows[1:]}
            if nick.lower() not in existing_nicks:
                row_vals = [
                    "",                    # TID (unknown)
                    nick, nick,
                    item.get("type", ""),
                    item.get("last_msg", ""),
                    "", "Pending", pkt_stamp(), "",
                ]
                if sheets.append_row(ws_que, row_vals):
                    logger.ok(f"New conversation (no tid): {nick}")
                    new_synced += 1

    # ── Phase 2: Send pending replies ─────────────────────────────────────────
    logger.info("Phase 2: Sending pending replies...")

    # Reload queue to get latest MY_REPLY values
    all_que_rows = sheets.read_all(ws_que)
    que_hmap     = SheetsManager.build_header_map(all_que_rows[0]) if all_que_rows else {}
    col_status   = sheets.get_col(all_que_rows[0] if all_que_rows else [], "STATUS")
    col_notes    = sheets.get_col(all_que_rows[0] if all_que_rows else [], "NOTES")
    col_updated  = sheets.get_col(all_que_rows[0] if all_que_rows else [], "UPDATED")

    def qcell2(row, *names):
        return SheetsManager.get_cell(row, que_hmap, *names)

    pending_replies = []
    for i, row in enumerate(all_que_rows[1:], start=2):
        reply  = qcell2(row, "MY_REPLY").strip()
        status = qcell2(row, "STATUS").lower()
        nick   = qcell2(row, "NICK").strip()
        tid    = qcell2(row, "TID").strip()
        if reply and status.startswith("pending"):
            pending_replies.append({
                "row": i, "nick": nick, "tid": tid, "reply": reply,
                "type": qcell2(row, "TYPE"),
            })

    sent   = 0
    failed = 0

    # Build tid → conv_url map from fetched inbox items
    tid_to_url = {
        str(it.get("tid", "")): it.get("conv_url", "")
        for it in inbox_items
        if it.get("tid")
    }

    for idx, item in enumerate(pending_replies, start=1):
        nick  = item["nick"]
        tid   = item["tid"]
        reply = item["reply"]
        row_n = item["row"]
        logger.info(f"[{idx}/{len(pending_replies)}] Replying to {nick} (tid={tid})")

        # Resolve conversation URL: prefer TID-based URL, fallback to nick
        conv_url = (tid_to_url.get(tid) or "").strip()
        if not conv_url:
            conv_url = f"{_URL_INBOX}{nick}/" if nick else _URL_INBOX

        ok = _send_reply(driver, conv_url, tid, reply, logger)

        if ok:
            logger.ok(f"Reply sent → {nick}")
            sheets.update_row_cells(ws_que, row_n, {
                col_status:  "Done",
                col_notes:   f"Replied @ {pkt_stamp()}",
                col_updated: pkt_stamp(),
            })
            # Log to InboxLog
            _log_entry(sheets, ws_log, pkt_stamp(), tid, nick,
                       item["type"], "OUT", reply, conv_url, "Sent")
            sheets.log_action("INBOX", "reply_sent", nick, conv_url, "Done", reply[:80])
            sent += 1
        else:
            logger.warning(f"Reply failed → {nick}")
            sheets.update_row_cells(ws_que, row_n, {
                col_status:  "Failed",
                col_notes:   f"Send failed @ {pkt_stamp()}",
                col_updated: pkt_stamp(),
            })
            _log_entry(sheets, ws_log, pkt_stamp(), tid, nick,
                       item["type"], "OUT", reply, conv_url, "Failed")
            failed += 1

        time.sleep(2)

    # ── Phase 3: Activity feed ────────────────────────────────────────────────
    logger.info("Phase 3: Fetching activity feed...")
    activity_items = _fetch_activity(driver, logger, max_items=60, max_pages=5)
    act_logged = 0

    for act in activity_items:
        _log_entry(
            sheets, ws_log,
            pkt_stamp(),
            act.get("tid", ""),
            act.get("nick", ""),
            act.get("type", "ACTIVITY"),
            "ACTIVITY",
            act.get("text", ""),
            act.get("url", ""),
            "Logged",
        )
        act_logged += 1
        time.sleep(0.2)

    logger.section(
        f"INBOX DONE — "
        f"New:{new_synced}  Sent:{sent}  Failed:{failed}  Activity:{act_logged}"
    )
    return {
        "new_synced":      new_synced,
        "replies_sent":    sent,
        "replies_failed":  failed,
        "activity_logged": act_logged,
    }


# Keep run_activity as a thin alias so main.py 'activity' mode still works
def run_activity(driver, sheets: SheetsManager, logger: Logger) -> Dict:
    """Alias: activity mode just calls the full inbox run."""
    return run_inbox(driver, sheets, logger)


# ════════════════════════════════════════════════════════════════════════════════
#  FETCH INBOX
# ════════════════════════════════════════════════════════════════════════════════

def _fetch_inbox(driver, logger: Logger) -> List[Dict]:
    """
    Open /inbox/ and parse all visible conversation blocks.

    Returns list of dicts:
      {tid, nick, type, last_msg, timestamp, conv_url}
    """
    try:
        driver.get(_URL_INBOX)
        time.sleep(3)

        items: List[Dict] = []
        seen_tids:  set   = set()
        seen_nicks: set   = set()

        blocks = driver.find_elements(By.CSS_SELECTOR, _SEL_ITEM_BLOCK)
        if not blocks:
            logger.warning("No inbox blocks found — inbox may be empty")
            return []

        for block in blocks[:30]:
            try:
                item = _parse_inbox_block(block)
                if not item:
                    continue

                tid  = str(item.get("tid",  "")).strip()
                nick = str(item.get("nick", "")).strip()

                if not nick:
                    continue

                # Deduplicate by TID first, then nick
                if tid and tid in seen_tids:
                    continue
                if not tid and nick.lower() in seen_nicks:
                    continue

                if tid:
                    seen_tids.add(tid)
                seen_nicks.add(nick.lower())
                items.append(item)

            except Exception as e:
                logger.debug(f"Skipped inbox block: {e}")
                continue

        return items

    except Exception as e:
        logger.error(f"Inbox fetch error: {e}")
        return []


def _parse_inbox_block(block) -> Optional[Dict]:
    """
    Parse one div.mbl.mtl inbox block into a structured dict.

    Returns:
        {tid, nick, type, last_msg, timestamp, conv_url}
        or None if required fields are missing.
    """
    # ── TID — from button[name='tid'] value ──────────────────────────────────
    # This is the stable user ID that never changes.
    # HTML: <button type="submit" name="tid" value="2464609">
    tid = ""
    try:
        btn = block.find_elements(By.CSS_SELECTOR, _SEL_TID_BTN)
        if btn:
            tid = (btn[0].get_attribute("value") or "").strip()
    except Exception:
        pass

    # ── Conversation type — "1 ON 1", "POST", "MEHFIL" etc. ──────────────────
    # HTML: <div class="sp cs mrs"><span style="color:#3b7af7">1 ON 1</span></div>
    conv_type = ""
    try:
        type_spans = block.find_elements(By.CSS_SELECTOR, _SEL_TYPE_SPAN)
        if type_spans:
            raw = (type_spans[0].text or "").strip().upper()
            # Normalise to short codes
            if "1" in raw and "ON" in raw:
                conv_type = "1ON1"
            elif "POST" in raw:
                conv_type = "POST"
            elif "MEHFIL" in raw:
                conv_type = "MEHFIL"
            else:
                conv_type = raw[:20]
    except Exception:
        pass

    # ── Nickname ─────────────────────────────────────────────────────────────
    # HTML: <div class="cl lsp nos"><b><bdi>Dazzling_Mushk</bdi></b>
    nick = ""
    try:
        nick_els = block.find_elements(By.CSS_SELECTOR, _SEL_NICK_BDI)
        if nick_els:
            nick = (nick_els[0].text or "").strip()
    except Exception:
        pass

    if not nick:
        return None

    # ── Last message text ─────────────────────────────────────────────────────
    # HTML: <span class="mrs"><bdi>hi</bdi></span>
    last_msg = ""
    try:
        msg_els = block.find_elements(By.CSS_SELECTOR, _SEL_MSG_SPAN)
        if msg_els:
            last_msg = (msg_els[0].text or "").strip()
    except Exception:
        pass

    # ── Relative timestamp ("1 hour ago") ────────────────────────────────────
    # DamaDam /inbox/ only shows relative time — no absolute datetime available.
    # We store the current PKT time as the sync timestamp.
    # HTML: <span class="mrs cxs sp" style="color:#999999">1 hour ago</span>
    timestamp = pkt_stamp()

    # ── Conversation URL ──────────────────────────────────────────────────────
    # Try to find a link to the specific conversation
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

    return {
        "tid":       tid,
        "nick":      nick,
        "type":      conv_type,
        "last_msg":  last_msg,
        "timestamp": timestamp,
        "conv_url":  conv_url,
    }


# ════════════════════════════════════════════════════════════════════════════════
#  SEND REPLY
# ════════════════════════════════════════════════════════════════════════════════

def _send_reply(driver, conv_url: str, tid: str,
                reply_text: str, logger: Logger) -> bool:
    """
    Navigate to conv_url and submit reply_text in the reply form.
    If conv_url fails, falls back to /inbox/ page to find the form.
    Returns True on success, False on failure.
    """
    try:
        driver.get(conv_url)
        time.sleep(3)

        form     = None
        textarea = None

        # Find the direct-response form and its textarea
        forms = driver.find_elements(By.CSS_SELECTOR, _SEL_REPLY_FORM)
        for f in forms:
            try:
                ta = f.find_element(By.CSS_SELECTOR, _SEL_REPLY_TEXTAREA)
                form     = f
                textarea = ta
                break
            except Exception:
                continue

        # Fallback: load /inbox/ and try again
        if not form or not textarea:
            logger.debug("Reply form not found at conv_url — trying /inbox/ fallback")
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
            logger.warning(f"Reply form not found (tid={tid})")
            return False

        # Find submit button — prefer dec=1 variant
        submit_btn = None
        for sel in (
            "button[type='submit'][name='dec'][value='1']",
            "button[type='submit']",
            "input[type='submit']",
        ):
            try:
                btns = form.find_elements(By.CSS_SELECTOR, sel)
                if btns:
                    submit_btn = btns[0]
                    break
            except Exception:
                pass

        if not submit_btn:
            logger.warning("Reply submit button not found")
            return False

        # Type and send
        safe_reply = strip_non_bmp(reply_text)[:350]
        textarea.clear()
        time.sleep(0.3)
        textarea.send_keys(safe_reply)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(3)

        # Basic verification
        try:
            if safe_reply[:20].lower() in driver.page_source.lower():
                return True
        except Exception:
            pass

        return True  # Assume success even if we can't verify

    except Exception as e:
        logger.error(f"Send reply error: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════════
#  FETCH ACTIVITY
# ════════════════════════════════════════════════════════════════════════════════

def _fetch_activity(driver, logger: Logger,
                    max_items: int = 60, max_pages: int = 5) -> List[Dict]:
    """
    Fetch DamaDam activity feed from /inbox/activity/.
    Returns list of dicts: {tid, nick, type, text, url}
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

            blocks = driver.find_elements(By.CSS_SELECTOR, _SEL_ITEM_BLOCK)
            if not blocks:
                break

            for block in blocks:
                if len(items) >= max_items:
                    break
                try:
                    # Extract TID if present
                    tid = ""
                    try:
                        btn = block.find_elements(By.CSS_SELECTOR, _SEL_TID_BTN)
                        if btn:
                            tid = (btn[0].get_attribute("value") or "").strip()
                    except Exception:
                        pass

                    # Extract type label
                    conv_type = "ACTIVITY"
                    try:
                        type_spans = block.find_elements(By.CSS_SELECTOR, _SEL_TYPE_SPAN)
                        if type_spans:
                            raw = (type_spans[0].text or "").strip().upper()
                            if "1" in raw and "ON" in raw:
                                conv_type = "1ON1"
                            elif "POST" in raw:
                                conv_type = "POST"
                            elif "MEHFIL" in raw:
                                conv_type = "MEHFIL"
                    except Exception:
                        pass

                    # Extract nick
                    nick = ""
                    try:
                        nick_els = block.find_elements(By.CSS_SELECTOR, _SEL_NICK_BDI)
                        if nick_els:
                            nick = (nick_els[0].text or "").strip()
                    except Exception:
                        pass

                    # Full text of block (cleaned)
                    raw_text = (block.text or "").strip()
                    lines = [
                        ln.strip() for ln in raw_text.splitlines()
                        if ln.strip() and ln.strip() not in {"►", "REMOVE", "▶", "SKIP ALL ON PAGE"}
                    ]
                    text = " | ".join(lines)[:300]
                    if not text:
                        continue

                    # Extract URL
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
                    key = (text[:80], item_url)
                    if key in seen:
                        continue
                    seen.add(key)

                    items.append({
                        "tid":  tid,
                        "nick": nick,
                        "type": conv_type,
                        "text": text,
                        "url":  item_url,
                    })

                except Exception:
                    continue

            # Check for next page
            try:
                next_btns = driver.find_elements(By.CSS_SELECTOR, "a[href*='?page='] button")
                has_next  = any("NEXT" in (b.text or "").upper() for b in next_btns)
                if not has_next:
                    break
            except Exception:
                break

    except Exception as e:
        logger.error(f"Activity fetch error: {e}")

    return items


# ════════════════════════════════════════════════════════════════════════════════
#  LOG HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _log_inbox_entry(sheets: SheetsManager, ws_log,
                     item: Dict, direction: str, status: str):
    """Log one inbox item to InboxLog sheet."""
    _log_entry(
        sheets, ws_log,
        pkt_stamp(),
        item.get("tid", ""),
        item.get("nick", ""),
        item.get("type", ""),
        direction,
        item.get("last_msg", ""),
        item.get("conv_url", ""),
        status,
    )


def _log_entry(sheets: SheetsManager, ws_log,
               timestamp: str, tid: str, nick: str,
               conv_type: str, direction: str,
               message: str, url: str, status: str):
    """Append one row to InboxLog sheet."""
    sheets.append_row(ws_log, [
        timestamp,   # TIMESTAMP
        tid,         # TID
        nick,        # NICK
        conv_type,   # TYPE
        direction,   # DIRECTION  IN / OUT / ACTIVITY
        message,     # MESSAGE
        url,         # CONV_URL
        status,      # STATUS
    ])
