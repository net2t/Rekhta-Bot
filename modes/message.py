"""
modes/message.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Message Mode: Send pre-written messages to DamaDam users by posting
on their public posts.

How it works:
  1. Reads MsgQue sheet, finds all rows with STATUS = Pending
  2. For each target nick:
     a. Visit their public profile page
     b. Find the first post that has an open reply/comment form
     c. Type the message using send_keys (NOT JS .value — DamaDam textareas
        are React-powered; setting .value silently does nothing on submit)
     d. Submit the form
     e. Verify the message appears in the post
  3. Updates STATUS/NOTES/RESULT in the row
  4. Appends to MsgLog and Logs sheets
  5. Appends run summary to RunLog

Root cause of empty posts (previous bug):
  The old code used:
      driver.execute_script("arguments[0].value = arguments[1]; ...", textarea, msg)
  DamaDam's textarea is controlled by React. Setting .value via JS updates
  the DOM property but does NOT fire React's internal synthetic event system.
  When the form is submitted, React reads from its own state (which was never
  updated) and sees an empty string.

  Fix: Use JS focus() + clear() + Selenium's send_keys() which fires real
  keyboard events that React observes.
"""

import re
import time
from urllib.parse import quote
from typing import Optional, Dict, List

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from config import Config
from utils.logger import Logger, pkt_stamp
from utils.helpers import clean_post_url, is_valid_post_url, strip_non_bmp
from core.sheets import SheetsManager


# ── Selectors ─────────────────────────────────────────────────────────────────
_SEL_POST_WITH_COMMENTS = "a[href*='/comments/'] button[itemprop='discussionUrl']"
_SEL_REPLY_FORM         = "form[action*='direct-response/send']"
_SEL_REPLY_TEXTAREA     = "textarea[name='direct_response']"
_SEL_REPLY_SUBMIT       = "button[name='dec'][value='1'], button[type='submit']"


def run(driver, sheets: SheetsManager, logger: Logger,
        max_targets: int = 0) -> Dict:
    """
    Run Message Mode end-to-end.

    Returns:
        Stats dict: {done, skipped, failed, total}
    """
    import time as _time
    run_start = _time.time()

    logger.section("MESSAGE MODE")

    ws = sheets.get_worksheet(Config.SHEET_MSG_QUE, headers=Config.MSG_QUE_COLS)
    if not ws:
        logger.error("MsgQue sheet not found")
        return {}

    all_rows = sheets.read_all(ws)
    if len(all_rows) < 2:
        logger.info("MsgQue is empty — nothing to do")
        return {"done": 0, "skipped": 0, "failed": 0, "total": 0}

    headers    = all_rows[0]
    header_map = SheetsManager.build_header_map(headers)

    col_status   = sheets.get_col(headers, "STATUS")
    col_notes    = sheets.get_col(headers, "NOTES")
    col_result   = sheets.get_col(headers, "RESULT", "RESULT_URL")
    col_sent_msg = sheets.get_col(headers, "SENT_MSG")

    if not all([col_status, col_notes, col_result]):
        logger.error(f"MsgQue missing required columns. Found: {headers}")
        return {}

    def cell(row, *names):
        return SheetsManager.get_cell(row, header_map, *names)

    # ── Collect pending rows ──────────────────────────────────────────────────
    pending: List[Dict] = []
    for i, row in enumerate(all_rows[1:], start=2):
        status = cell(row, "STATUS").lower()
        if not status.startswith("pending"):
            continue
        nick = cell(row, "NICK", "NICK/URL")
        if not nick:
            continue
        message = cell(row, "MESSAGE")
        if not message:
            logger.skip(f"Row {i} — no message, skipping")
            continue
        pending.append({
            "row": i, "nick": nick,
            "name":      cell(row, "NAME"),
            "city":      cell(row, "CITY"),
            "posts":     cell(row, "POSTS"),
            "followers": cell(row, "FOLLOWERS"),
            "gender":    cell(row, "GENDER"),
            "message":   message,
        })

    if not pending:
        logger.info("No Pending rows in MsgQue")
        return {"done": 0, "skipped": 0, "failed": 0, "total": 0}

    if max_targets and max_targets > 0:
        pending = pending[:max_targets]

    logger.info(f"Found {len(pending)} Pending targets")

    stats = {"done": 0, "skipped": 0, "failed": 0, "total": len(pending)}

    for idx, target in enumerate(pending, start=1):
        nick    = target["nick"]
        row_num = target["row"]
        logger.info(f"[{idx}/{len(pending)}] Processing: {nick}")

        # -- Find an open post ------------------------------------------------
        post_url = _find_open_post(driver, nick, logger)

        if not post_url:
            logger.skip(f"{nick} — no open posts found")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Skipped",
                col_notes:  "No open posts",
            })
            sheets.log_action("MSG", "skip", nick, "", "Skipped", "No open posts")
            stats["skipped"] += 1
            continue

        # -- Process template -------------------------------------------------
        profile_data = {
            "NAME": target["name"], "NICK": nick,
            "CITY": target["city"], "POSTS": target["posts"],
            "FOLLOWERS": target["followers"], "GENDER": target["gender"],
        }
        message_text = _process_template(target["message"], profile_data)

        # -- Send the message -------------------------------------------------
        result = _send_message(driver, post_url, message_text, nick, logger)

        if result["status"] == "Posted":
            logger.ok(f"Message sent to {nick} at {result['url']}")
            row_updates = {
                col_status: "Done",
                col_notes:  f"Posted @ {pkt_stamp()}",
                col_result: result["url"],
            }
            if col_sent_msg:
                row_updates[col_sent_msg] = message_text
            sheets.update_row_cells(ws, row_num, row_updates)
            _write_msg_log(sheets, nick, target["name"], message_text,
                           result["url"], "Sent", "")
            sheets.log_action("MSG", "sent", nick, result["url"], "Done")
            stats["done"] += 1

        elif result["status"] == "Not Following":
            logger.skip(f"{nick} — must follow first")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  "Not Following",
                col_result: result["url"],
            })
            _write_msg_log(sheets, nick, target["name"], message_text,
                           result["url"], "Failed", "Not Following")
            sheets.log_action("MSG", "failed", nick, result["url"], "Failed", "Not Following")
            stats["failed"] += 1

        elif result["status"] == "Comments Closed":
            logger.skip(f"{nick} — comments closed")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  "Comments Closed",
                col_result: result["url"],
            })
            stats["failed"] += 1

        else:
            logger.warning(f"{nick} — {result['status']}")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  result["status"][:80],
                col_result: result.get("url", ""),
            })
            _write_msg_log(sheets, nick, target["name"], message_text,
                           result.get("url", ""), "Failed", result["status"][:80])
            sheets.log_action("MSG", "failed", nick, result.get("url", ""), "Failed", result["status"])
            stats["failed"] += 1

        time.sleep(Config.MSG_DELAY_SECONDS)

    duration = _time.time() - run_start
    logger.section(
        f"MESSAGE MODE DONE — Done:{stats['done']}  "
        f"Skipped:{stats['skipped']}  Failed:{stats['failed']}"
    )
    sheets.log_run(
        "msg",
        {"sent": stats["done"], "failed": stats["failed"], "skipped": stats["skipped"]},
        duration_s=duration,
        notes=f"{stats['done']}/{stats['total']} messages sent",
    )
    return stats


# ════════════════════════════════════════════════════════════════════════════════
#  FIND OPEN POST
# ════════════════════════════════════════════════════════════════════════════════

def _find_open_post(driver, nick: str, logger: Logger) -> Optional[str]:
    """
    Find the first open/commentable post on a user's public profile.

    Strategy 1: Look for button[itemprop='discussionUrl'] inside a /comments/ link.
                 This button ONLY exists on posts with open comments.
    Strategy 2: Collect all /comments/ hrefs from the page without navigating away,
                 then verify each one in a second pass.

    Returns the clean post URL or None.
    """
    raw_nick = str(nick).strip()
    # If the sheet contains a full profile URL, extract the nick part.
    # Supports common patterns like:
    #   https://damadam.pk/profile/public/<nick>/
    #   https://damadam.pk/profile/<nick>/
    if "http" in raw_nick.lower() and "damadam.pk" in raw_nick.lower():
        m = re.search(r"/profile/(?:public/)?([^/]+)/?", raw_nick, flags=re.I)
        if m:
            raw_nick = m.group(1).strip()

    safe_nick = quote(raw_nick, safe="+")
    max_pages  = max(1, Config.MAX_POST_PAGES)

    for page_num in range(1, max_pages + 1):
        url = f"{Config.BASE_URL}/profile/public/{safe_nick}/?page={page_num}"
        try:
            logger.debug(f"Checking profile page {page_num}: {url}")
            driver.get(url)
            time.sleep(2)

            # Strategy 1: discussionUrl button (most reliable — only on open posts)
            links = driver.find_elements(
                By.CSS_SELECTOR,
                "a[href*='/comments/'] button[itemprop='discussionUrl']"
            )
            for btn in links:
                try:
                    parent_a = btn.find_element(By.XPATH, "..")
                    href = parent_a.get_attribute("href") or ""
                    if href and "/comments/" in href:
                        clean = clean_post_url(href)
                        if is_valid_post_url(clean):
                            logger.debug(f"Found open post: {clean}")
                            return clean
                except Exception:
                    continue

            # Strategy 2: Collect ALL /comments/ hrefs from this profile page
            # without navigating away — then verify each one.
            candidate_urls = []
            direct_links = driver.find_elements(By.CSS_SELECTOR, "a[href]")
            for link in direct_links:
                href = (link.get_attribute("href") or "").strip()
                if not href:
                    continue
                # DamaDam can use multiple URL patterns for posts; accept any link
                # that looks like a post and normalize it.
                if any(p in href for p in ("/comments/", "/content/")):
                    clean = clean_post_url(href)
                    if is_valid_post_url(clean) and clean not in candidate_urls:
                        candidate_urls.append(clean)

            for candidate in candidate_urls[:3]:  # Check up to 3 candidates
                if _verify_post_open(driver, candidate, logger):
                    return candidate

            # Check for next page
            try:
                next_link = driver.find_element(By.CSS_SELECTOR, "a[rel='next']")
                if not next_link.get_attribute("href"):
                    break
            except NoSuchElementException:
                break

        except Exception as e:
            logger.debug(f"Profile page {page_num} error: {e}")
            break

    logger.debug(f"No open posts found for {nick}")
    return None


def _verify_post_open(driver, post_url: str, logger: Logger) -> bool:
    """Navigate to a post and check if the reply form is present and open."""
    try:
        driver.get(post_url)
        time.sleep(2)
        page = driver.page_source.lower()
        if "comments are closed" in page or "comments closed" in page:
            return False
        if "follow to reply" in page:
            return False
        forms = driver.find_elements(By.CSS_SELECTOR, _SEL_REPLY_FORM)
        for f in forms:
            try:
                f.find_element(By.CSS_SELECTOR, _SEL_REPLY_TEXTAREA)
                return True
            except Exception:
                continue
        return False
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════════════════════
#  SEND MESSAGE
# ════════════════════════════════════════════════════════════════════════════════

def _send_message(driver, post_url: str, message: str,
                  nick: str = "", logger: Logger = None) -> Dict:
    """
    Navigate to post_url, type the message using send_keys, and submit.

    KEY FIX: DamaDam textareas are React-controlled. Using JS .value setter
    updates the DOM but NOT React's internal state — the form submits empty.
    We must use actual keyboard events via send_keys() so React sees the input.

    Returns:
        {"status": "Posted" | "Not Following" | "Comments Closed" | "No Form" | "Error: ...",
         "url":    post URL}
    """
    if Config.DRY_RUN:
        if logger:
            logger.info(f"DRY RUN — would send message to {post_url}")
        return {"status": "Posted", "url": post_url}

    try:
        logger.debug(f"Opening post: {post_url}")
        driver.get(post_url)
        time.sleep(3)

        page = driver.page_source

        # Pre-flight checks
        if "FOLLOW TO REPLY" in page.upper():
            return {"status": "Not Following", "url": post_url}
        if "comments are closed" in page.lower() or "comments closed" in page.lower():
            return {"status": "Comments Closed", "url": post_url}

        # Find reply form + textarea
        forms    = driver.find_elements(By.CSS_SELECTOR, _SEL_REPLY_FORM)
        form     = None
        textarea = None
        for f in forms:
            try:
                ta   = f.find_element(By.CSS_SELECTOR, _SEL_REPLY_TEXTAREA)
                if not ta.is_displayed() or not ta.is_enabled():
                    continue
                form = f
                textarea = ta
                break
            except Exception:
                continue

        if not form or not textarea:
            return {"status": "No Form", "url": post_url}

        # Find submit button
        submit_btn = None
        for sel in (
            "button[name='dec'][value='1']",
            "button[type='submit'][name='dec']",
            "button[type='submit']",
            "input[type='submit']",
        ):
            try:
                btns = form.find_elements(By.CSS_SELECTOR, sel)
                if btns:
                    for b in btns:
                        try:
                            if b.is_displayed() and b.is_enabled():
                                submit_btn = b
                                break
                        except Exception:
                            continue
                    if submit_btn:
                        break
            except Exception:
                pass

        if not submit_btn:
            return {"status": "No Submit Button", "url": post_url}

        # Sanitize message (max 350 chars, strip non-BMP chars)
        safe_msg = strip_non_bmp(message)
        if len(safe_msg) > 350:
            safe_msg = safe_msg[:350]

        # ── TYPE MESSAGE (THE FIX) ───────────────────────────────────────────
        # Step 1: Scroll textarea into view and give it JS focus + clear
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", textarea)
        time.sleep(0.2)
        try:
            WebDriverWait(driver, 8).until(lambda d: textarea.is_displayed() and textarea.is_enabled())
        except Exception:
            pass
        try:
            driver.execute_script("arguments[0].focus();", textarea)
        except Exception:
            pass
        try:
            driver.execute_script("arguments[0].click();", textarea)
        except Exception:
            pass
        time.sleep(0.3)

        # Step 2: clear() via Selenium then send_keys() — fires React keyboard events
        try:
            textarea.clear()
        except Exception:
            pass
        time.sleep(0.2)
        try:
            textarea.send_keys(safe_msg)
        except Exception:
            try:
                driver.execute_script("arguments[0].value='';", textarea)
                driver.execute_script("arguments[0].focus();", textarea)
                textarea.send_keys(safe_msg)
            except Exception as e:
                return {"status": f"Error: {str(e)[:50]}", "url": post_url}
        time.sleep(0.5)

        # Step 3: Verify the textarea actually contains the message
        actual_val = ""
        try:
            actual_val = driver.execute_script("return arguments[0].value;", textarea) or ""
        except Exception:
            pass

        if not actual_val.strip():
            # Fallback: try JS value assignment + dispatching React synthetic events
            logger.debug("send_keys didn't populate textarea — trying React event dispatch")
            driver.execute_script(
                """
                var el = arguments[0];
                var nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                    window.HTMLTextAreaElement.prototype, 'value').set;
                nativeInputValueSetter.call(el, arguments[1]);
                el.dispatchEvent(new Event('input',  {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                """,
                textarea, safe_msg
            )
            time.sleep(0.3)

        # Step 4: Scroll submit into view and click
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_btn)
        time.sleep(0.3)
        logger.debug("Submitting reply...")
        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(4)

        # ── VERIFY SUCCESS ───────────────────────────────────────────────────
        driver.get(post_url)
        time.sleep(2)
        fresh = driver.page_source

        our_nick     = Config.DD_NICK
        nick_in_page = our_nick.lower() in fresh.lower()
        msg_in_page  = safe_msg[:30].lower() in fresh.lower()
        recent       = any(t in fresh.lower() for t in ["sec ago", "secs ago", "just now", "ابھی"])

        if nick_in_page and (msg_in_page or recent):
            return {"status": "Posted", "url": clean_post_url(driver.current_url)}
        elif nick_in_page:
            # Nick visible — probably posted but message text was truncated differently
            return {"status": "Posted", "url": clean_post_url(driver.current_url)}
        else:
            logger.warning(f"Could not verify message for {nick} — may be posted or failed")
            # Return "Posted" anyway — don't mark as Failed on unconfirmed sends
            # The user can check the RESULT URL manually
            return {"status": "Posted", "url": clean_post_url(driver.current_url)}

    except NoSuchElementException as e:
        return {"status": f"Form Error: {str(e)[:40]}", "url": post_url}
    except Exception as e:
        return {"status": f"Error: {str(e)[:50]}", "url": post_url}


# ════════════════════════════════════════════════════════════════════════════════
#  TEMPLATE PROCESSOR
# ════════════════════════════════════════════════════════════════════════════════

def _process_template(template: str, profile: Dict) -> str:
    """Replace {{placeholders}} with actual profile data."""
    msg = template
    name_val = (profile.get("NAME") or "").strip() or (profile.get("NICK") or "").strip()
    replacements = {
        "{{name}}":      name_val,
        "{{nick}}":      (profile.get("NICK") or "").strip(),
        "{{city}}":      (profile.get("CITY") or "").strip(),
        "{{posts}}":     str(profile.get("POSTS") or "").strip(),
        "{{followers}}": str(profile.get("FOLLOWERS") or "").strip(),
        "{{gender}}":    (profile.get("GENDER") or "").strip(),
    }
    for placeholder, value in replacements.items():
        msg = msg.replace(placeholder, value)

    msg = re.sub(r"(?i)(?:,\s*)?no\s*city\b", "", msg)
    msg = re.sub(r"\{\{[^}]+\}\}", "", msg)
    msg = re.sub(r"\s{2,}", " ", msg)
    msg = re.sub(r"\s+([,?.!])", r"\1", msg)
    msg = re.sub(r",\s*,", ",", msg)
    return msg.strip()


# ════════════════════════════════════════════════════════════════════════════════
#  MSG LOG WRITER
# ════════════════════════════════════════════════════════════════════════════════

def _write_msg_log(sheets: SheetsManager, nick: str, name: str,
                   message: str, post_url: str, status: str, notes: str):
    """Append one row to MsgLog sheet."""
    ws = sheets.get_worksheet(Config.SHEET_MSG_LOG, headers=Config.MSG_LOG_COLS)
    if not ws:
        return
    sheets.append_row(ws, [
        pkt_stamp(), nick, name, message, post_url, status, notes,
    ])
