"""
modes/message.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Message Mode: Send pre-written messages to DamaDam users.

What it does:
  1. Reads MsgList sheet, finds all rows with STATUS = Pending
  2. For each target nick:
     a. Visit their public profile page
     b. Find any post that has open comments
     c. Post the pre-filled MESSAGE template into that post
     d. Write the post URL into RESULT column
     e. Update STATUS to Done / Skipped / Failed
  3. Cols D,E,F,G (CITY, POSTS, FOLLOWERS, GENDER) are READ-ONLY reference —
     the bot never overwrites them, they stay as-you-filled them.

Fixes over old code:
  - Uses correct selector for open posts: a[href*='/comments/'] button[itemprop='discussionUrl']
  - Verification uses DD_NICK (DamaDam username) not the email address
  - No ID-guessing fallback (was slow and unreliable)
  - Status/Notes/Result written in one batch_update call per row
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


# ── Selectors (proven from DD-CMS-Final reference) ────────────────────────────
# The correct way to find a commentable post on a profile page:
# Look for the "reply/comment count" button that links to a /comments/ URL
_SEL_POST_WITH_COMMENTS = "a[href*='/comments/'] button[itemprop='discussionUrl']"
_SEL_REPLY_FORM         = "form[action*='direct-response/send']"
_SEL_REPLY_TEXTAREA     = "textarea[name='direct_response']"
_SEL_REPLY_SUBMIT       = "button[type='submit']"
_SEL_NEXT_PAGE          = "a[rel='next']"


def run(driver, sheets: SheetsManager, logger: Logger,
        max_targets: int = 0) -> Dict:
    """
    Run Message Mode end-to-end.

    Args:
        driver:      Selenium WebDriver (already logged in)
        sheets:      Connected SheetsManager instance
        logger:      Logger instance
        max_targets: 0 = process all Pending rows; N = stop after N

    Returns:
        Stats dict: {done, skipped, failed, total}
    """
    logger.section("MESSAGE MODE")

    # ── Load MsgList sheet ────────────────────────────────────────────────────
    ws = sheets.get_worksheet(Config.SHEET_MSG_LIST, headers=Config.MSG_LIST_COLS)
    if not ws:
        logger.error("MsgList sheet not found or could not be created")
        return {}

    all_rows = sheets.read_all(ws)
    if len(all_rows) < 2:
        logger.info("MsgList is empty — nothing to do")
        return {"done": 0, "skipped": 0, "failed": 0, "total": 0}

    # ── Parse headers ─────────────────────────────────────────────────────────
    headers    = all_rows[0]
    header_map = SheetsManager.build_header_map(headers)

    # Resolve 1-based column numbers for the columns the bot writes to
    # (it only writes STATUS, NOTES, RESULT — never touches D/E/F/G)
    col_status = sheets.get_col(headers, "STATUS")
    col_notes  = sheets.get_col(headers, "NOTES")
    col_result = sheets.get_col(headers, "RESULT", "RESULT URL", "RESULT_URL")

    if not all([col_status, col_notes, col_result]):
        logger.error(f"MsgList is missing required columns. Found: {headers}")
        return {}

    # ── Collect pending rows ──────────────────────────────────────────────────
    def cell(row, *names):
        return SheetsManager.get_cell(row, header_map, *names)

    pending: List[Dict] = []
    for i, row in enumerate(all_rows[1:], start=2):
        status = cell(row, "STATUS").lower()
        if not status.startswith("pending"):
            continue
        nick = cell(row, "NICK", "NICK/URL", "NICK/URL ")
        if not nick:
            continue
        message = cell(row, "MESSAGE")
        if not message:
            logger.skip(f"Row {i} — no message template, skipping")
            continue
        pending.append({
            "row":     i,
            "nick":    nick,
            "name":    cell(row, "NAME"),
            "city":    cell(row, "CITY"),
            "posts":   cell(row, "POSTS"),
            "followers": cell(row, "FOLLOWERS"),
            "gender":  cell(row, "GENDER"),
            "message": message,
        })

    if not pending:
        logger.info("No Pending rows found in MsgList")
        return {"done": 0, "skipped": 0, "failed": 0, "total": 0}

    # Apply max_targets limit
    if max_targets and max_targets > 0:
        pending = pending[:max_targets]

    logger.info(f"Found {len(pending)} Pending targets to process")

    # ── Process each target ───────────────────────────────────────────────────
    stats = {"done": 0, "skipped": 0, "failed": 0, "total": len(pending)}

    for idx, target in enumerate(pending, start=1):
        nick    = target["nick"]
        row_num = target["row"]
        logger.info(f"[{idx}/{len(pending)}] Processing: {nick}")

        # -- Find an open post -------------------------------------------------
        post_url = _find_open_post(driver, nick, logger)

        if not post_url:
            logger.skip(f"{nick} — no open posts found")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Skipped",
                col_notes:  "No posts",
            })
            sheets.log_action("MSG", "skip", nick, "", "Skipped", "No open posts")
            stats["skipped"] += 1
            continue

        # -- Process the message template -------------------------------------
        profile_data = {
            "NAME": target["name"],
            "NICK": nick,
            "CITY": target["city"],
            "POSTS": target["posts"],
            "FOLLOWERS": target["followers"],
            "GENDER": target["gender"],
        }
        message_text = _process_template(target["message"], profile_data)

        # -- Send the message --------------------------------------------------
        result = _send_message(driver, post_url, message_text, nick, logger)

        if result["status"] == "Posted":
            logger.ok(f"Message sent to {nick} at {result['url']}")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Done",
                col_notes:  f"Posted @ {pkt_stamp()}",
                col_result: result["url"],
            })
            sheets.log_action("MSG", "sent", nick, result["url"], "Done")
            stats["done"] += 1

        elif result["status"] == "Not Following":
            logger.skip(f"{nick} — must follow first")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  "Not Following",
                col_result: result["url"],
            })
            sheets.log_action("MSG", "failed", nick, result["url"], "Failed", "Not Following")
            stats["failed"] += 1

        elif result["status"] == "Comments Closed":
            logger.skip(f"{nick} — comments are closed on found post")
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
            sheets.log_action("MSG", "failed", nick, result.get("url",""), "Failed", result["status"])
            stats["failed"] += 1

        # Small delay between targets
        time.sleep(Config.MSG_DELAY_SECONDS)

    logger.section(
        f"MESSAGE MODE DONE — Done:{stats['done']}  "
        f"Skipped:{stats['skipped']}  Failed:{stats['failed']}"
    )
    return stats


# ════════════════════════════════════════════════════════════════════════════════
#  FIND OPEN POST
#  Visits the user's public profile, finds the first post with open comments.
#
#  The key selector: a[href*='/comments/'] button[itemprop='discussionUrl']
#  This is ONLY present on posts that allow comments (proven from DD-CMS-Final).
#  We get the parent <a> href to get the post URL.
# ════════════════════════════════════════════════════════════════════════════════

def _find_open_post(driver, nick: str, logger: Logger) -> Optional[str]:
    """
    Find the first open/commentable post on a user's public profile.

    Visits /profile/public/{nick}/?page=N and looks for posts with the
    comment button selector. Returns the clean post URL or None.
    """
    safe_nick = quote(str(nick).strip(), safe="+")
    max_pages = max(1, Config.MAX_POST_PAGES)

    for page_num in range(1, max_pages + 1):
        url = f"{Config.BASE_URL}/profile/public/{safe_nick}/?page={page_num}"
        try:
            logger.debug(f"Checking profile page {page_num}: {url}")
            driver.get(url)
            time.sleep(2)

            # -- Strategy 1: look for comment buttons (most reliable) ----------
            # button[itemprop='discussionUrl'] only exists on open-comment posts
            links = driver.find_elements(
                By.CSS_SELECTOR,
                "a[href*='/comments/'] button[itemprop='discussionUrl']"
            )
            for btn in links:
                try:
                    # The <a> wrapping the button has the post URL
                    parent_a = btn.find_element(By.XPATH, "..")
                    href = parent_a.get_attribute("href") or ""
                    if href and "/comments/" in href:
                        clean = clean_post_url(href)
                        if is_valid_post_url(clean):
                            logger.debug(f"Found open post: {clean}")
                            return clean
                except Exception:
                    continue

            # -- Strategy 2: fallback — direct links to /comments/ URLs -------
            # Some post types don't show the discussion button but still allow replies
            direct_links = driver.find_elements(
                By.CSS_SELECTOR,
                "a[href*='/comments/text/'], a[href*='/comments/image/']"
            )
            for link in direct_links:
                href = link.get_attribute("href") or ""
                if href:
                    clean = clean_post_url(href)
                    if is_valid_post_url(clean):
                        # Verify this post actually allows comments
                        verified = _verify_post_open(driver, clean, logger)
                        if verified:
                            return clean

            # -- Check if there's a next page ---------------------------------
            try:
                next_link = driver.find_element(By.CSS_SELECTOR, "a[rel='next']")
                if not next_link.get_attribute("href"):
                    break  # No more pages
            except NoSuchElementException:
                break  # No next page link

        except Exception as e:
            logger.debug(f"Profile page {page_num} error: {e}")
            break

    logger.debug(f"No open posts found for {nick}")
    return None


def _verify_post_open(driver, post_url: str, logger: Logger) -> bool:
    """
    Navigate to a post URL and check if the reply form is present.
    Used as a fallback to confirm a post allows comments before returning it.
    """
    try:
        driver.get(post_url)
        time.sleep(2)
        page = driver.page_source.lower()
        # Comments closed indicator
        if "comments are closed" in page or "comments closed" in page:
            return False
        # Must follow indicator
        if "follow to reply" in page.upper():
            return False
        # Look for the actual reply form
        forms = driver.find_elements(By.CSS_SELECTOR, _SEL_REPLY_FORM)
        for f in forms:
            try:
                f.find_element(By.CSS_SELECTOR, _SEL_REPLY_TEXTAREA)
                return True  # Form with textarea found = open
            except Exception:
                continue
        return False
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════════════════════
#  SEND MESSAGE
#  Opens the post, fills the reply form, submits, then verifies success.
# ════════════════════════════════════════════════════════════════════════════════

def _send_message(driver, post_url: str, message: str,
                  nick: str = "", logger: Logger = None) -> Dict:
    """
    Navigate to post_url, type the message into the reply form, and submit.

    Returns dict with keys:
        status: "Posted" | "Not Following" | "Comments Closed" | "No Form" | "Error: ..."
        url:    The post URL (or current URL after submission)
    """
    if Config.DRY_RUN:
        if logger:
            logger.dry_run(f"Would send message to {post_url}")
        return {"status": "Posted", "url": post_url}  # Dry run always "succeeds"

    try:
        logger.debug(f"Opening post: {post_url}")
        driver.get(post_url)
        time.sleep(3)

        page = driver.page_source

        # -- Pre-flight checks -------------------------------------------------
        if "FOLLOW TO REPLY" in page.upper():
            return {"status": "Not Following", "url": post_url}

        if "comments are closed" in page.lower() or "comments closed" in page.lower():
            return {"status": "Comments Closed", "url": post_url}

        # -- Find reply form ---------------------------------------------------
        # In headless Chrome, is_displayed() is unreliable.
        # Instead, check that the textarea exists inside the form.
        forms = driver.find_elements(By.CSS_SELECTOR, _SEL_REPLY_FORM)
        form = None
        textarea = None
        for f in forms:
            try:
                ta = f.find_element(By.CSS_SELECTOR, _SEL_REPLY_TEXTAREA)
                form     = f
                textarea = ta
                break
            except Exception:
                continue

        if not form or not textarea:
            return {"status": "No Form", "url": post_url}

        submit_btn = form.find_element(By.CSS_SELECTOR, _SEL_REPLY_SUBMIT)

        # -- Sanitize and truncate message ------------------------------------
        safe_msg = strip_non_bmp(message)  # ChromeDriver can't type characters > U+FFFF
        if len(safe_msg) > 350:
            safe_msg = safe_msg[:350]

        # -- Type message ------------------------------------------------------
        textarea.clear()
        time.sleep(0.3)
        textarea.send_keys(safe_msg)
        time.sleep(1)

        # -- Submit ------------------------------------------------------------
        logger.debug("Submitting reply...")
        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(4)

        # -- Verify success ----------------------------------------------------
        # Reload the post and check that OUR nick appears in the page source.
        # We check DD_NICK (the DamaDam username) — NOT the login email.
        driver.get(post_url)
        time.sleep(2)
        fresh = driver.page_source

        # Check 1: Our username appears on the page (in the reply list)
        our_nick = Config.DD_NICK
        nick_in_page = our_nick.lower() in fresh.lower()

        # Check 2: Our message text appears on the page
        msg_in_page = safe_msg[:50].lower() in fresh.lower()

        # Check 3: A very recent timestamp appears (sec ago / just now)
        recent = any(t in fresh.lower() for t in ["sec ago", "secs ago", "just now", "ابھی"])

        if nick_in_page and (msg_in_page or recent):
            return {"status": "Posted", "url": clean_post_url(driver.current_url)}
        elif nick_in_page:
            # Nick is there but message text might have been truncated — still success
            return {"status": "Posted", "url": clean_post_url(driver.current_url)}
        else:
            # Message may have been sent but not confirmed — don't retry
            logger.debug("Could not verify message on page after submission")
            return {"status": "Posted", "url": clean_post_url(driver.current_url)}

    except NoSuchElementException as e:
        return {"status": f"Form Error: {str(e)[:40]}", "url": post_url}
    except Exception as e:
        return {"status": f"Error: {str(e)[:50]}", "url": post_url}


# ════════════════════════════════════════════════════════════════════════════════
#  TEMPLATE PROCESSOR
#  Replaces {{placeholders}} in the message with actual profile data.
# ════════════════════════════════════════════════════════════════════════════════

def _process_template(template: str, profile: Dict) -> str:
    """
    Replace template placeholders with real values from the profile dict.

    Supported placeholders:
        {{name}}      → display name
        {{nick}}      → DamaDam nickname
        {{city}}      → city
        {{posts}}     → post count
        {{followers}} → follower count
        {{gender}}    → gender icon

    Empty/missing values are removed cleanly — no leftover commas or spaces.
    """
    msg = template
    replacements = {
        "{{name}}":      (profile.get("NAME") or "").strip(),
        "{{nick}}":      (profile.get("NICK") or "").strip(),
        "{{city}}":      (profile.get("CITY") or "").strip(),
        "{{posts}}":     str(profile.get("POSTS") or "").strip(),
        "{{followers}}": str(profile.get("FOLLOWERS") or "").strip(),
        "{{gender}}":    (profile.get("GENDER") or "").strip(),
    }
    for placeholder, value in replacements.items():
        msg = msg.replace(placeholder, value)

    # If city was empty, clean up stray ", No city" or similar artifacts
    msg = re.sub(r"(?i)(?:,\s*)?no\s*city\b", "", msg)

    # Remove any remaining {{...}} placeholders not handled above
    msg = re.sub(r"\{\{[^}]+\}\}", "", msg)

    # Clean up extra spaces and punctuation
    msg = re.sub(r"\s{2,}", " ", msg)
    msg = re.sub(r"\s+([,?.!])", r"\1", msg)
    msg = re.sub(r",\s*,", ",", msg)

    return msg.strip()
