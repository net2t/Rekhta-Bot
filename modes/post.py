"""
modes/post.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Post Mode: Create new posts on DamaDam from PostQueue sheet.

FIXED in V2.3.1 — Root cause confirmed from logs:
  "Page didn't change after file select — proceeding anyway"

  The bot was sending the file path to the input BUT DamaDam's
  upload handler was NOT firing. This means the image was never
  actually uploaded — the form was submitted empty/broken.
  DamaDam accepted it once (luck/cache) and rejected it on retry.

  TWO targeted fixes in _create_image_post() only:

  FIX 1 — Force upload handler to fire after send_keys
    Old: send_keys(path) → wait 10s → "proceeding anyway" → blind submit
    New: send_keys(path) → dispatch JS 'change'+'input' events explicitly
         → wait for page response → retry with JS click if no response
         → return False if STILL no response after both attempts

  FIX 2 — Do NOT submit if upload not confirmed
    Old: "Page didn't change... proceeding anyway" → submit → FAIL
    New: if upload not confirmed → return "Upload Not Confirmed"
         Row marked Failed in sheet, cooldown skipped, bot moves on.

  Everything else is UNCHANGED from V2.2.0.
"""

import os
import re
import time
import tempfile
from datetime import datetime
from typing import Optional, Dict, List, Set

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from config import Config
from utils.logger import Logger, pkt_stamp, PKT
from utils.helpers import (
    download_image, sanitize_caption, sanitize_tags,
    strip_non_bmp, clean_post_url, is_share_or_denied_url
)
from core.sheets import SheetsManager


# ── DamaDam share page URLs ────────────────────────────────────────────────────
_URL_IMAGE_UPLOAD = f"{Config.BASE_URL}/share/photo/upload/"
_URL_TEXT_SHARE   = f"{Config.BASE_URL}/share/text/"

# Minimum caption length DamaDam accepts
_MIN_CAPTION_LEN = 5


# ════════════════════════════════════════════════════════════════════════════════
#  FORENSIC DUMP — only when DD_DEBUG=1
# ════════════════════════════════════════════════════════════════════════════════

def _dump(driver, logger: Logger, label: str) -> None:
    if not Config.DEBUG:
        return
    try:
        ts   = time.strftime("%H%M%S")
        base = os.path.join(str(Config.LOG_DIR), f"post_{ts}_{label}")
        try:
            driver.save_screenshot(base + ".png")
        except Exception:
            pass
        try:
            with open(base + ".html", "w", encoding="utf-8", errors="replace") as f:
                f.write(f"<!-- URL: {driver.current_url} -->\n")
                f.write(driver.page_source or "")
        except Exception:
            pass
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════════
#  CAPTION VALIDATION — UNCHANGED
# ════════════════════════════════════════════════════════════════════════════════

def _validate_caption(caption: str) -> tuple[bool, str]:
    """
    Validate caption before posting.
    Returns (is_valid, reason_if_invalid).
    """
    if not caption or not caption.strip():
        return False, "Caption is empty"

    c = caption.strip()

    if len(c) < _MIN_CAPTION_LEN:
        return False, f"Caption too short ({len(c)} chars, min {_MIN_CAPTION_LEN})"

    max_run = Config.POST_MAX_REPEAT_CHARS
    run     = 1
    for i in range(1, len(c)):
        if c[i] == c[i - 1]:
            run += 1
            if run > max_run:
                return False, f"Caption has repeated char '{c[i]}' ({run}+ times in a row)"
        else:
            run = 1

    return True, ""


# ════════════════════════════════════════════════════════════════════════════════
#  NEW FUNCTION — _trigger_file_upload()
#
#  This is the CORE FIX. It replaces the old bare send_keys() block.
#
#  WHY THE OLD CODE FAILED (confirmed from logs):
#    "Page didn't change after file select — proceeding anyway"
#    Chrome's send_keys() sets the file input value BUT does not always
#    fire the 'change' DOM event — especially on repeat navigations to
#    the same page. DamaDam's upload handler listens for 'change'.
#    No 'change' event = handler never runs = page never changes = no upload.
#    Bot then submitted a broken/empty form. DamaDam accepted it once,
#    blocked it the second time as a duplicate of the empty submission.
#
#  WHAT THIS FUNCTION DOES DIFFERENTLY:
#    Step A: Expose hidden file input (same as before)
#    Step B: send_keys(file_path) (same as before)
#    Step C: Explicitly dispatch 'input' + 'change' via JavaScript
#            — this guarantees the upload handler fires every time
#    Step D: Wait up to 8s for page to respond (upload in progress)
#    Step E: If still no response, try once more via JS .click() + send_keys
#    Step F: Wait another 5s after second attempt
#    Step G: Return True if page responded at any point, False if never
# ════════════════════════════════════════════════════════════════════════════════

def _trigger_file_upload(driver, file_input, abs_path: str, logger: Logger) -> bool:
    """
    Send file to input AND confirm DamaDam's upload handler actually fired.

    Returns True  — page responded, upload started (safe to proceed)
    Returns False — page never responded (do NOT submit — would be empty post)
    """

    # ── Step A: Expose the hidden file input ─────────────────────────────────
    # DamaDam hides <input type='file'> with CSS.
    # We must make it accessible for send_keys() to work.
    try:
        driver.execute_script("""
            var el = arguments[0];
            el.style.display  = 'block';
            el.style.opacity  = '1';
            el.style.position = 'fixed';
            el.style.top      = '0';
            el.style.left     = '0';
            el.style.width    = '200px';
            el.style.height   = '50px';
            el.style.zIndex   = '99999';
        """, file_input)
        time.sleep(0.4)
    except Exception as e:
        logger.warning(f"Could not expose file input: {e}")

    # ── Step B: Send the file path ────────────────────────────────────────────
    try:
        file_input.send_keys(abs_path)
        logger.info("File path sent to input via send_keys")
        time.sleep(0.5)
    except Exception as e:
        logger.error(f"send_keys to file input failed: {e}")
        return False

    # ── Step C: Dispatch DOM events explicitly ────────────────────────────────
    # THIS IS THE KEY FIX.
    # Chrome does not reliably auto-fire 'change' after send_keys on hidden
    # inputs, especially on repeated navigations to the same page.
    # DamaDam's JS upload handler is bound to the 'change' event.
    # Without this dispatch, the handler never runs → page never changes.
    try:
        driver.execute_script("""
            var el = arguments[0];
            // 'input' fires first in most frameworks
            el.dispatchEvent(new Event('input',  { bubbles: true, cancelable: true }));
            // 'change' is what DamaDam's upload handler specifically listens for
            el.dispatchEvent(new Event('change', { bubbles: true, cancelable: true }));
        """, file_input)
        logger.debug("Dispatched 'input' + 'change' events on file input")
    except Exception as e:
        logger.warning(f"Event dispatch failed: {e} — upload may not start")

    # ── Step D: Wait up to 8s for page to respond ────────────────────────────
    # If the handler received the file, the page will change within a few
    # seconds (preview image appears, or some DOM element updates).
    page_before = driver.page_source
    for tick in range(8):
        time.sleep(1)
        try:
            page_now = driver.page_source
            if page_now != page_before:
                logger.info(f"Upload confirmed — page changed after {tick + 1}s ✓")
                return True   # SUCCESS — handler fired, upload started
            page_before = page_now
        except Exception:
            pass

    # ── Step E: Second attempt via JS click ───────────────────────────────────
    # Some DamaDam page versions wrap the file input in a styled button/label.
    # If the first send_keys had no visible effect, clicking the input element
    # directly via JS can re-trigger the browser's file selection mechanism,
    # after which we immediately send the path again.
    logger.warning(
        "Page did not respond to first file selection attempt — "
        "trying JS click fallback (attempt 2/2)..."
    )
    try:
        driver.execute_script("arguments[0].click();", file_input)
        time.sleep(0.8)
        file_input.send_keys(abs_path)
        time.sleep(0.5)
        # Fire events again after the second send_keys
        driver.execute_script("""
            var el = arguments[0];
            el.dispatchEvent(new Event('input',  { bubbles: true, cancelable: true }));
            el.dispatchEvent(new Event('change', { bubbles: true, cancelable: true }));
        """, file_input)
        logger.debug("Second attempt: events dispatched after JS click")
    except Exception as e:
        logger.warning(f"JS click fallback failed: {e}")

    # ── Step F: Wait another 5s after second attempt ─────────────────────────
    page_before2 = driver.page_source
    for tick in range(5):
        time.sleep(1)
        try:
            page_now2 = driver.page_source
            if page_now2 != page_before2:
                logger.info(f"Upload confirmed on 2nd attempt after {tick + 1}s ✓")
                return True   # SUCCESS on second attempt
            page_before2 = page_now2
        except Exception:
            pass

    # ── Step G: Both attempts failed ─────────────────────────────────────────
    # Page never responded to file selection after two attempts with event
    # dispatch. Returning False tells _create_image_post() to abort this row.
    # This prevents submitting an empty form to DamaDam.
    logger.warning(
        "Upload handler did not fire after 2 attempts and event dispatch. "
        "Aborting this row to prevent empty/broken post submission."
    )
    return False


# ════════════════════════════════════════════════════════════════════════════════
#  MAIN RUN — unchanged except new "Upload Not Confirmed" status handler
# ════════════════════════════════════════════════════════════════════════════════

def run(driver, sheets: SheetsManager, logger: Logger,
        max_posts: int = 0,
        stop_on_fail: bool = False,
        force_wait: int | None = None) -> Dict:
    """Run Post Mode end-to-end."""
    import time as _time
    run_start = _time.time()

    logger.section("POST MODE")

    ws = sheets.get_worksheet(Config.SHEET_POST_QUE, headers=Config.POST_QUE_COLS)
    if not ws:
        logger.error("PostQueue sheet not found")
        return {}

    all_rows = sheets.read_all(ws)
    if len(all_rows) < 2:
        logger.info("PostQueue is empty — nothing to do")
        return {"posted": 0, "skipped": 0, "failed": 0, "total": 0}

    headers    = all_rows[0]
    header_map = SheetsManager.build_header_map(headers)

    col_status   = sheets.get_col(headers, "STATUS")
    col_post_url = sheets.get_col(headers, "POST_URL")
    col_notes    = sheets.get_col(headers, "NOTES")

    if not all([col_status, col_post_url, col_notes]):
        logger.error(f"PostQueue missing required columns. Found: {headers}")
        return {}

    def cell(row, *names):
        return SheetsManager.get_cell(row, header_map, *names)

    if force_wait and force_wait > 0 and not Config.DRY_RUN:
        logger.info(f"Force wait: {force_wait}s before starting...")
        time.sleep(force_wait)

    col_img_link  = sheets.get_col(headers, "IMG_LINK")
    posted_urls: Set[str] = set()
    if col_img_link:
        for row in all_rows[1:]:
            if cell(row, "STATUS").lower() == "done":
                img = cell(row, "IMG_LINK")
                if img:
                    posted_urls.add(img.lower())
        logger.info(f"Duplicate index: {len(posted_urls)} already-posted images")

    pending: List[Dict] = []
    dup_count = 0

    for i, row in enumerate(all_rows[1:], start=2):
        st = cell(row, "STATUS").lower()
        if not st.startswith("pending"):
            continue

        post_type = cell(row, "TYPE").lower()
        if post_type not in ("image", "text"):
            continue

        img_link = cell(row, "IMG_LINK")

        if post_type == "image" and img_link and img_link.lower() in posted_urls:
            dup_count += 1
            sheets.update_row_cells(ws, i, {
                col_status: "Repeating",
                col_notes:  "Duplicate IMG_LINK (pre-check)",
            })
            continue

        urdu_raw = cell(row, "URDU")
        if urdu_raw.startswith("="):
            urdu_raw = ""

        pending.append({
            "row":      i,
            "type":     post_type,
            "img_link": img_link,
            "urdu":     urdu_raw,
            "title":    cell(row, "TITLE"),
            "poet":     cell(row, "POET"),
        })

    if dup_count:
        logger.info(f"Skipped {dup_count} duplicate rows (pre-check)")

    if not pending:
        logger.info("No Pending rows to post")
        return {"posted": 0, "skipped": 0, "failed": 0, "total": 0}

    if max_posts and max_posts > 0:
        pending = pending[:max_posts]

    logger.info(f"Will process {len(pending)} rows")

    stats = {"posted": 0, "skipped": 0, "failed": 0, "total": len(pending)}
    last_post_time: float = 0.0

    for idx, item in enumerate(pending, start=1):
        row_num   = item["row"]
        post_type = item["type"]
        img_link  = item["img_link"]
        logger.info(f"[{idx}/{len(pending)}] Row {row_num} — type={post_type}")

        caption = _build_caption(item)
        cap_valid, cap_reason = _validate_caption(caption)

        if not cap_valid:
            logger.warning(f"[POST] Row {row_num} — caption invalid: {cap_reason}")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Skipped",
                col_notes:  f"Bad caption: {cap_reason}",
            })
            stats["skipped"] += 1
            continue

        if last_post_time > 0 and not Config.DRY_RUN:
            elapsed  = time.time() - last_post_time
            required = 185
            if elapsed < required:
                wait = required - elapsed
                logger.info(f"[WAIT] Cooldown {wait:.0f}s (last success {elapsed:.0f}s ago)")
                time.sleep(wait)

        if post_type == "image":
            if not img_link:
                logger.warning(f"[POST] Row {row_num} — no IMG_LINK, skipping")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Skipped",
                    col_notes:  "No IMG_LINK",
                })
                stats["skipped"] += 1
                continue
            result = _create_image_post(driver, img_link, caption, logger)
        else:
            content = item["urdu"] or item["title"]
            if not content:
                logger.warning(f"[POST] Row {row_num} — no text content, skipping")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Skipped",
                    col_notes:  "No content",
                })
                stats["skipped"] += 1
                continue
            result = _create_text_post(driver, content, logger)

        status   = result.get("status", "Error")
        post_url = result.get("url", "")

        if status == "Posted":
            logger.ok(f"[POST] Success → {post_url}")
            sheets.update_row_cells(ws, row_num, {
                col_status:   "Done",
                col_post_url: post_url,
                col_notes:    f"Posted @ {pkt_stamp()}",
            })
            _write_post_log(sheets, item, post_url, "Posted", "")
            posted_urls.add((img_link or "").lower())
            last_post_time = time.time()
            stats["posted"] += 1

        elif status == "Dry Run":
            sheets.update_row_cells(ws, row_num, {
                col_status: "Skipped",
                col_notes:  "Dry run",
            })
            stats["skipped"] += 1

        elif status == "Repeating":
            logger.warning(f"[POST] Row {row_num} — duplicate image (DamaDam rejected)")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Repeating",
                col_notes:  "DamaDam: duplicate image",
            })
            _write_post_log(sheets, item, post_url, "Repeating", "Duplicate image")
            stats["skipped"] += 1

        elif status == "Caption Error":
            logger.warning(f"[POST] Row {row_num} — caption rejected by DamaDam")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  "DamaDam: caption error (repeated chars/spam)",
            })
            _write_post_log(sheets, item, post_url, "Failed", "Caption Error")
            stats["failed"] += 1

        # ── NEW: Upload handler never fired — FIX 2 ──────────────────────────
        # This replaces the old silent "proceeding anyway" path.
        # Nothing was sent to DamaDam, so no cooldown is applied.
        # Row is marked Failed so user knows to investigate the image URL.
        elif status == "Upload Not Confirmed":
            logger.warning(f"[POST] Row {row_num} — upload handler did not fire, skipping")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  "Upload handler did not respond — image not sent to DamaDam",
            })
            _write_post_log(sheets, item, "", "Failed", "Upload not confirmed")
            stats["failed"] += 1
            # NO cooldown — DamaDam received nothing from us

        elif status == "Rate Limited":
            wait_s = int(result.get("wait_seconds") or Config.POST_COOLDOWN_SECONDS)
            logger.warning(f"[WAIT] Cooldown {wait_s}s — DamaDam rate limit active")
            logger.info(f"[WAIT] Sleeping {wait_s}s then retrying row {row_num} once...")
            time.sleep(wait_s + 5)

            if post_type == "image":
                result2 = _create_image_post(driver, img_link, caption, logger)
            else:
                result2 = _create_text_post(driver, item["urdu"] or item["title"], logger)

            status2   = result2.get("status", "Error")
            post_url2 = result2.get("url", "")

            if status2 == "Posted":
                logger.ok(f"[POST] Retry success → {post_url2}")
                sheets.update_row_cells(ws, row_num, {
                    col_status:   "Done",
                    col_post_url: post_url2,
                    col_notes:    f"Posted (retry) @ {pkt_stamp()}",
                })
                _write_post_log(sheets, item, post_url2, "Posted", "retry after rate limit")
                posted_urls.add((img_link or "").lower())
                last_post_time = time.time()
                stats["posted"] += 1
            else:
                note = f"Rate limited x2 @ {pkt_stamp()} — will retry next run"
                logger.warning(f"[POST] Row {row_num} — {note}")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Pending",
                    col_notes:  note[:80],
                })
                break

        else:
            logger.error(f"[POST] Row {row_num} failed: {status}")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  status[:80],
            })
            _write_post_log(sheets, item, post_url, "Failed", status[:80])
            stats["failed"] += 1
            if stop_on_fail:
                break

    duration = _time.time() - run_start
    logger.section(
        f"POST MODE DONE — Posted:{stats['posted']}  "
        f"Skipped:{stats['skipped']}  Failed:{stats['failed']}"
    )
    return stats


# ════════════════════════════════════════════════════════════════════════════════
#  CREATE IMAGE POST — FIX 1 + FIX 2 applied here
# ════════════════════════════════════════════════════════════════════════════════

def _create_image_post(driver, img_url: str, caption: str, logger: Logger) -> Dict:
    """
    Upload an image to DamaDam and publish it.

    Changes vs V2.2.0 (Steps 3–5 only, rest unchanged):

      Step 3: Find file input — same selectors, same logic
      Step 4: OLD: make_visible + send_keys + 10s wait + "proceeding anyway"
              NEW: _trigger_file_upload() — sends file, dispatches DOM events,
                   waits for response, retries — returns True/False
      Step 5: NEW gate — if _trigger_file_upload() returns False:
                         return "Upload Not Confirmed" immediately
                         (old code had no gate — always submitted)
    """
    tmp_path = ""
    try:
        # ── Step 1: Download image ────────────────────────────────────────────
        logger.info(f"Downloading: {img_url[:80]}")
        tmp_path = download_image(img_url, logger)
        abs_path = os.path.abspath(tmp_path)
        logger.info(f"Image saved: {abs_path}")

        # ── Step 2: Navigate to upload page ───────────────────────────────────
        logger.info(f"Opening upload page: {_URL_IMAGE_UPLOAD}")
        driver.get(_URL_IMAGE_UPLOAD)
        time.sleep(3)
        _dump(driver, logger, "01_upload_page")

        cur = driver.current_url.lower()
        if "login" in cur:
            return {"status": "Login Required", "url": driver.current_url}

        if "upload-denied" in cur or "denied" in cur:
            wait_s = _parse_countdown_seconds(driver.page_source)
            logger.warning(f"Upload-denied on page load — cooldown {wait_s}s active")
            return {"status": "Rate Limited", "url": driver.current_url, "wait_seconds": wait_s}

        # ── Step 3: Find file input ───────────────────────────────────────────
        file_input = None
        for sel in (
            "input[type='file'][name='image']",
            "input[type='file'][name='file']",
            "input[type='file'][name='photo']",
            "input[type='file']",
        ):
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                file_input = els[0]
                logger.info(f"File input found via: {sel}")
                break

        if not file_input:
            _dump(driver, logger, "ERROR_no_file_input")
            return {"status": "Form Error: no file input found", "url": driver.current_url}

        # ── Step 4: Trigger upload with confirmed handler fire (FIX 1) ────────
        #
        # OLD CODE (removed — this was the bug):
        #   driver.execute_script("...make visible...", file_input)
        #   file_input.send_keys(abs_path)
        #   for tick in range(10):
        #       time.sleep(1)
        #       if page changed: break
        #   logger.info("Page didn't change after file select — proceeding anyway")
        #                                                          ↑ BUG HERE
        # NEW CODE:
        #   _trigger_file_upload() dispatches DOM events so handler fires.
        #   Returns False if page never responds → we abort instead of submit.
        #
        upload_ok = _trigger_file_upload(driver, file_input, abs_path, logger)

        # ── Step 5: Gate — abort if upload not confirmed (FIX 2) ─────────────
        #
        # OLD CODE: no gate existed — always fell through to caption + submit
        # NEW CODE: hard stop here — return explicit status for run() to handle
        #
        if not upload_ok:
            _dump(driver, logger, "ERROR_upload_not_confirmed")
            return {"status": "Upload Not Confirmed", "url": driver.current_url}

        # ── Step 6: Fill caption ───────────────────────────────────────────────
        clean_cap = sanitize_caption(strip_non_bmp(caption))
        cap_filled = _fill_textarea(driver, logger, clean_cap)
        if not cap_filled:
            logger.warning("Caption fill failed — posting without caption")

        # ── Step 7: Radio options ─────────────────────────────────────────────
        _click_radio_label(driver, logger, "exp-first", "Never expire post")
        _click_radio_label(driver, logger, "com-off",   "Turn Off Replies: Yes")
        _dump(driver, logger, "03_before_submit")

        # ── Step 8: Submit ────────────────────────────────────────────────────
        if Config.DRY_RUN:
            logger.info("DRY RUN — not submitting")
            return {"status": "Dry Run", "url": driver.current_url}

        submit = _find_submit_button(driver, logger)
        if not submit:
            _dump(driver, logger, "ERROR_no_submit")
            return {"status": "Form Error: no submit button found", "url": driver.current_url}

        url_before = driver.current_url
        logger.info(f"Clicking submit (URL before: {url_before})")
        driver.execute_script("arguments[0].click();", submit)

        # ── Step 9: Wait for redirect ─────────────────────────────────────────
        redirected = False
        for tick in range(30):
            time.sleep(1)
            try:
                cur_url = driver.current_url
                if cur_url != url_before:
                    logger.info(f"Redirected after {tick+1}s → {cur_url}")
                    redirected = True
                    break
            except Exception:
                pass

        time.sleep(2)
        _dump(driver, logger, "04_after_submit")

        final_url = driver.current_url
        page_src  = driver.page_source
        page_low  = page_src.lower()
        logger.info(f"Final URL: {final_url}")

        # ── Step 10: Determine result ─────────────────────────────────────────

        if _detect_repeating_image(page_low) or "upload-denied" in final_url.lower():
            if _detect_caption_error(page_low):
                return {"status": "Caption Error", "url": final_url}
            return {"status": "Repeating", "url": final_url}

        if _detect_caption_error(page_low):
            return {"status": "Caption Error", "url": final_url}

        wait_s = _detect_rate_limit(page_low)
        if wait_s:
            logger.warning(f"Rate limit detected — page says wait {wait_s}s")
            return {"status": "Rate Limited", "url": final_url, "wait_seconds": wait_s}

        if redirected and "upload" not in final_url.lower():
            post_url = _extract_post_url(driver)
            return {"status": "Posted", "url": post_url}

        if "upload" in final_url.lower() or "share" in final_url.lower():
            err = _extract_error_message(driver)
            logger.error(f"Still on upload page. Error: {err}")
            return {"status": f"Upload Error: {err}", "url": final_url}

        post_url = _extract_post_url(driver)
        return {"status": "Posted", "url": post_url}

    except RuntimeError as e:
        return {"status": f"Image Download Failed: {str(e)[:60]}", "url": ""}
    except Exception as e:
        try:
            _dump(driver, logger, "EXCEPTION_image_post")
        except Exception:
            pass
        return {"status": f"Error: {str(e)[:60]}", "url": ""}
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════════
#  CREATE TEXT POST — UNCHANGED from V2.2.0
# ════════════════════════════════════════════════════════════════════════════════

def _create_text_post(driver, content: str, logger: Logger) -> Dict:
    """Create a text post on DamaDam. Unchanged from V2.2.0."""
    try:
        driver.get(_URL_TEXT_SHARE)
        time.sleep(3)
        _dump(driver, logger, "01_text_share_page")

        cur = driver.current_url.lower()
        if "login" in cur:
            return {"status": "Login Required", "url": driver.current_url}

        clean_content = sanitize_caption(strip_non_bmp(content))
        filled = _fill_textarea(driver, logger, clean_content)
        if not filled:
            return {"status": "Text form: textarea not found", "url": driver.current_url}

        _click_radio_label(driver, logger, "exp-first", "Never expire post")
        _click_radio_label(driver, logger, "com-off",   "Turn Off Replies: Yes")
        _dump(driver, logger, "02_text_before_submit")

        if Config.DRY_RUN:
            return {"status": "Dry Run", "url": driver.current_url}

        submit = _find_submit_button(driver, logger)
        if not submit:
            return {"status": "Form Error: no submit button", "url": driver.current_url}

        url_before = driver.current_url
        driver.execute_script("arguments[0].click();", submit)

        redirected = False
        for tick in range(30):
            time.sleep(1)
            try:
                if driver.current_url != url_before:
                    redirected = True
                    break
            except Exception:
                pass

        time.sleep(2)
        _dump(driver, logger, "03_text_after_submit")

        final_url = driver.current_url
        page_low  = driver.page_source.lower()

        if _detect_caption_error(page_low):
            return {"status": "Caption Error", "url": final_url}

        wait_s = _detect_rate_limit(page_low)
        if wait_s:
            return {"status": "Rate Limited", "url": final_url, "wait_seconds": wait_s}

        if redirected and "share" not in final_url.lower():
            return {"status": "Posted", "url": _extract_post_url(driver)}

        err = _extract_error_message(driver)
        return {"status": f"Submit Error: {err}", "url": final_url}

    except Exception as e:
        try:
            _dump(driver, logger, "EXCEPTION_text_post")
        except Exception:
            pass
        return {"status": f"Error: {str(e)[:60]}", "url": ""}


# ════════════════════════════════════════════════════════════════════════════════
#  DETECTION HELPERS — UNCHANGED from V2.2.0
# ════════════════════════════════════════════════════════════════════════════════

def _detect_repeating_image(page_source_lower: str) -> bool:
    """Detect duplicate-image rejection page."""
    indicators = [
        "duplicate image",
        "pehle upload ho chuka",
        "kuch new upload karein",
        "already posted",
        "dobara", "doosri baar",
    ]
    return any(ind in page_source_lower for ind in indicators)


def _detect_caption_error(page_source_lower: str) -> bool:
    """Detect caption-rejection page."""
    indicators = [
        "cannot share image",
        "itni zyada dafa aik hi character",
        "image ko sahi se describe karein",
        "wapis jaien aur dubara try karein",
    ]
    return any(ind in page_source_lower for ind in indicators)


def _detect_rate_limit(page_source_lower: str) -> int:
    """
    Detect DamaDam cooldown and parse exact seconds to wait.
    Returns seconds to wait, or 0 if no rate limit detected.
    """
    # Strip <script> blocks — prevents New Relic JS false positives
    src = re.sub(r'<script[\s\S]*?</script>', '', page_source_lower, flags=re.IGNORECASE)

    m = re.search(r'(\d+)\s*sec\b', src)
    if m:
        return max(int(m.group(1)) + 5, 30)

    m = re.search(r'(\d+)\s*min\b', src)
    if m:
        return int(m.group(1)) * 60 + 10

    for phrase in (
        "ap image share kar sakein ge",
        "you are posting too fast",
        "wait before posting",
        "post limit reached",
        "too many posts",
        "posting limit",
        "1 min baad",
        "2 min baad",
    ):
        if phrase in src:
            return Config.POST_COOLDOWN_SECONDS

    return 0


def _parse_countdown_seconds(page_source: str) -> int:
    """Parse cooldown seconds from DamaDam's upload-denied page on initial load."""
    result = _detect_rate_limit(page_source.lower())
    return result if result else Config.POST_COOLDOWN_SECONDS


# ════════════════════════════════════════════════════════════════════════════════
#  FORM HELPERS — UNCHANGED from V2.2.0
# ════════════════════════════════════════════════════════════════════════════════

def _fill_textarea(driver, logger: Logger, text: str) -> bool:
    """Find a textarea on the page and fill it."""
    selectors = [
        "textarea#pub_img_caption_field",
        "textarea[name='caption']",
        "textarea[name='description']",
        "textarea[name='text']",
        "textarea[name='body']",
        "textarea[name='content']",
        "textarea",
    ]

    for sel in selectors:
        try:
            areas = driver.find_elements(By.CSS_SELECTOR, sel)
            for area in areas:
                try:
                    if not area.is_displayed():
                        continue
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", area)
                    time.sleep(0.3)
                    try:
                        area.clear()
                    except Exception:
                        pass
                    driver.execute_script("arguments[0].value = '';", area)
                    time.sleep(0.2)
                    area.send_keys(text)
                    time.sleep(0.3)
                    actual = driver.execute_script("return arguments[0].value;", area) or ""
                    if actual.strip():
                        logger.info(f"Caption filled ({len(actual)} chars) via [{sel}]")
                        return True
                    # React event fallback
                    driver.execute_script("""
                        var el  = arguments[0];
                        var val = arguments[1];
                        var setter = Object.getOwnPropertyDescriptor(
                            window.HTMLTextAreaElement.prototype, 'value').set;
                        setter.call(el, val);
                        el.dispatchEvent(new Event('input',  {bubbles: true}));
                        el.dispatchEvent(new Event('change', {bubbles: true}));
                    """, area, text)
                    time.sleep(0.3)
                    actual2 = driver.execute_script("return arguments[0].value;", area) or ""
                    if actual2.strip():
                        logger.info(f"Caption filled via React event ({len(actual2)} chars)")
                        return True
                except Exception as e:
                    logger.debug(f"Textarea attempt [{sel}]: {e}")
                    continue
        except Exception:
            continue

    logger.warning("Could not fill any textarea")
    return False


def _find_submit_button(driver, logger: Logger):
    """Find the submit/share button on the DamaDam upload form."""
    selectors = [
        "button#share_img_btn",
        "button[name='btn'][value='1']",
        "input[name='btn'][value='1']",
        "button[type='submit']",
        "input[type='submit']",
        "button.btn-primary",
    ]
    for sel in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                try:
                    if el.is_displayed() and el.is_enabled():
                        logger.info(f"Submit button found via: {sel}")
                        return el
                except Exception:
                    continue
        except Exception:
            continue

    try:
        for btn in driver.find_elements(By.CSS_SELECTOR, "button, input[type='submit']"):
            try:
                txt = (btn.text or btn.get_attribute("value") or "").strip().lower()
                if any(kw in txt for kw in ("share", "post", "submit", "upload", "publish")):
                    if btn.is_displayed() and btn.is_enabled():
                        logger.info(f"Submit button found by text: '{txt}'")
                        return btn
            except Exception:
                continue
    except Exception:
        pass

    return None


def _click_radio_label(driver, logger: Logger, label_for: str, description: str = ""):
    """Click a DamaDam radio option by its label[for='...'] element."""
    try:
        label = driver.find_element(By.CSS_SELECTOR, f"label[for='{label_for}']")
        driver.execute_script("arguments[0].click();", label)
        logger.info(f"Radio selected: {description} (label[for={label_for!r}])")
    except Exception as e:
        logger.warning(f"Could not select radio '{description}': {e}")


# ════════════════════════════════════════════════════════════════════════════════
#  POST URL EXTRACTION — UNCHANGED from V2.2.0
# ════════════════════════════════════════════════════════════════════════════════

def _extract_post_url(driver) -> str:
    """Extract the newly created post URL from the page after redirect."""
    try:
        og = driver.find_elements(By.CSS_SELECTOR, "meta[property='og:url']")
        if og:
            href = (og[0].get_attribute("content") or "").strip()
            if href and "/comments/" in href:
                return clean_post_url(href)
    except Exception:
        pass

    current = driver.current_url
    if any(p in current for p in ("/comments/image/", "/comments/text/", "/content/")):
        return clean_post_url(current)

    try:
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/comments/'], a[href*='/content/']")
        for a in links:
            href = (a.get_attribute("href") or "").strip()
            if href and "damadam.pk" in href:
                return clean_post_url(href)
    except Exception:
        pass

    try:
        m = re.search(
            r"https?://[^\s\"']*(/comments/(?:text|image)/\d+|/content/\d+)",
            driver.page_source
        )
        if m:
            return clean_post_url(m.group(0))
    except Exception:
        pass

    return clean_post_url(current)


def _extract_error_message(driver) -> str:
    """Try to find a visible error message on the current page."""
    for sel in (".errorlist li", ".alert-danger", ".error", "div.err",
                "p.error", "span.error", ".messages li", ".alert"):
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                text = (els[0].text or "").strip()
                if text:
                    return text[:100]
        except Exception:
            pass
    return "unknown — check debug dumps"


# ════════════════════════════════════════════════════════════════════════════════
#  CAPTION BUILDER — UNCHANGED from V2.2.0
# ════════════════════════════════════════════════════════════════════════════════

def _build_caption(item: Dict) -> str:
    """Build the post caption from the URDU column (plain text only)."""
    parts = []
    urdu = (item.get("urdu") or "").strip()

    if urdu and not urdu.startswith("="):
        parts.append(urdu)

    if Config.POST_SIGNATURE:
        parts.append(Config.POST_SIGNATURE)

    return "\n".join(parts)


# ════════════════════════════════════════════════════════════════════════════════
#  POST LOG — UNCHANGED from V2.2.0
# ════════════════════════════════════════════════════════════════════════════════

def _write_post_log(sheets: SheetsManager, item: Dict,
                    post_url: str, status: str, notes: str):
    """Append one row to PostLog sheet after every post attempt."""
    ws = sheets.get_worksheet(Config.SHEET_POST_LOG, headers=Config.POST_LOG_COLS)
    if not ws:
        return
    sheets.append_row(ws, [
        pkt_stamp(),
        item.get("type", ""),
        item.get("poet", ""),
        (item.get("title") or "")[:80],
        post_url,
        item.get("img_link", ""),
        status,
        notes[:100] if notes else "",
    ])
