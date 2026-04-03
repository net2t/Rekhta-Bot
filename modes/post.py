"""
modes/post.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Post Mode: Create new posts on DamaDam from PostQueue sheet.

FIXED in V2.2.0:
  - URDU column formula removed — reads plain text value directly (manual paste workflow)
  - Caption validation: skip row if empty, too short (<5 chars), or repeated chars
  - Duplicate image: correctly detects upload-denied page with English heading
  - Caption error: detects "Cannot share image!" repeated-char rejection
  - Rate limit / cooldown timer: parses seconds from page counter, waits exactly that
  - Cooldown ONLY applied after SUCCESS — failures skip immediately to next row
  - TOO_FAST error on upload page (counter shown) now correctly waits + retries once
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
#  CAPTION VALIDATION
# ════════════════════════════════════════════════════════════════════════════════

def _validate_caption(caption: str) -> tuple[bool, str]:
    """
    Validate caption before posting.

    Returns (is_valid, reason_if_invalid).

    Rules (mirrors DamaDam server-side checks):
      1. Not empty
      2. At least _MIN_CAPTION_LEN chars after stripping
      3. No word/char repeated more than POST_MAX_REPEAT_CHARS times in a row
         (DamaDam blocks e.g. "zzzzzzz" or "______")
    """
    if not caption or not caption.strip():
        return False, "Caption is empty"

    c = caption.strip()

    if len(c) < _MIN_CAPTION_LEN:
        return False, f"Caption too short ({len(c)} chars, min {_MIN_CAPTION_LEN})"

    # Check for repeated characters (same char more than max_run times in a row)
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
#  MAIN RUN
# ════════════════════════════════════════════════════════════════════════════════

def run(driver, sheets: SheetsManager, logger: Logger,
        max_posts: int = 0,
        stop_on_fail: bool = False,
        force_wait: int | None = None) -> Dict:
    """
    Run Post Mode end-to-end.
    """
    import time as _time
    run_start = _time.time()

    logger.section("POST MODE")

    # ── Load PostQueue sheet ──────────────────────────────────────────────────
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

    # ── Pre-run force wait ────────────────────────────────────────────────────
    if force_wait and force_wait > 0 and not Config.DRY_RUN:
        logger.info(f"Force wait: {force_wait}s before starting...")
        time.sleep(force_wait)

    # ── Build duplicate index ─────────────────────────────────────────────────
    col_img_link  = sheets.get_col(headers, "IMG_LINK")
    posted_urls: Set[str] = set()
    if col_img_link:
        for row in all_rows[1:]:
            if cell(row, "STATUS").lower() == "done":
                img = cell(row, "IMG_LINK")
                if img:
                    posted_urls.add(img.lower())
        logger.info(f"Duplicate index: {len(posted_urls)} already-posted images")

    # ── Collect pending rows ──────────────────────────────────────────────────
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

        # Duplicate image check (local sheet — fast pre-filter)
        if post_type == "image" and img_link and img_link.lower() in posted_urls:
            dup_count += 1
            sheets.update_row_cells(ws, i, {
                col_status: "Repeating",
                col_notes:  "Duplicate IMG_LINK (pre-check)",
            })
            continue

        # Read URDU column as plain text (no formula processing)
        # Bot expects manual paste — if value starts with '=' it's unevaluated
        urdu_raw = cell(row, "URDU")
        if urdu_raw.startswith("="):
            urdu_raw = ""   # formula not evaluated yet — treat as empty

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

    # ── Process each row ──────────────────────────────────────────────────────
    stats = {"posted": 0, "skipped": 0, "failed": 0, "total": len(pending)}
    last_post_time: float = 0.0

    for idx, item in enumerate(pending, start=1):
        row_num   = item["row"]
        post_type = item["type"]
        img_link  = item["img_link"]
        logger.info(f"[{idx}/{len(pending)}] Row {row_num} — type={post_type}")

        # ── Build and validate caption BEFORE touching browser ────────────────
        caption = _build_caption(item)
        cap_valid, cap_reason = _validate_caption(caption)

        if not cap_valid:
            logger.warning(f"[POST] Row {row_num} — caption invalid: {cap_reason}")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Skipped",
                col_notes:  f"Bad caption: {cap_reason}",
            })
            stats["skipped"] += 1
            continue  # NO cooldown — just skip

        # ── Cooldown: only after a SUCCESSFUL post ────────────────────────────
        # DamaDam enforces ~2 min between posts. We use 185s for safety.
        # RULE: apply ONLY when last action was a success.
        if last_post_time > 0 and not Config.DRY_RUN:
            elapsed  = time.time() - last_post_time
            required = 185
            if elapsed < required:
                wait = required - elapsed
                logger.info(f"[WAIT] Cooldown {wait:.0f}s (last success {elapsed:.0f}s ago)")
                time.sleep(wait)

        # ── Create post ───────────────────────────────────────────────────────
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

        # ── Handle result ─────────────────────────────────────────────────────
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
            last_post_time = time.time()   # ← cooldown timer starts HERE only
            stats["posted"] += 1

        elif status == "Dry Run":
            sheets.update_row_cells(ws, row_num, {
                col_status: "Skipped",
                col_notes:  "Dry run",
            })
            stats["skipped"] += 1

        elif status == "Repeating":
            # DamaDam rejected — duplicate image detected on their side
            logger.warning(f"[POST] Row {row_num} — duplicate image (DamaDam rejected)")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Repeating",
                col_notes:  "DamaDam: duplicate image",
            })
            _write_post_log(sheets, item, post_url, "Repeating", "Duplicate image")
            stats["skipped"] += 1
            # NO cooldown — move immediately to next

        elif status == "Caption Error":
            # DamaDam rejected caption (repeated chars, spam, etc.)
            logger.warning(f"[POST] Row {row_num} — caption rejected by DamaDam")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  "DamaDam: caption error (repeated chars/spam)",
            })
            _write_post_log(sheets, item, post_url, "Failed", "Caption Error")
            stats["failed"] += 1
            # NO cooldown

        elif status == "Rate Limited":
            # DamaDam cooldown active — parse wait time from result
            wait_s = int(result.get("wait_seconds") or Config.POST_COOLDOWN_SECONDS)
            logger.warning(f"[WAIT] Cooldown {wait_s}s — DamaDam rate limit active")
            logger.info(f"[WAIT] Sleeping {wait_s}s then retrying row {row_num} once...")
            time.sleep(wait_s + 5)   # +5 buffer

            # Retry once
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
                # Still failing — mark pending, stop this run
                note = f"Rate limited x2 @ {pkt_stamp()} — will retry next run"
                logger.warning(f"[POST] Row {row_num} — {note}")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Pending",
                    col_notes:  note[:80],
                })
                break   # Stop run — DamaDam is blocking hard

        else:
            # Any other error
            logger.error(f"[POST] Row {row_num} failed: {status}")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  status[:80],
            })
            _write_post_log(sheets, item, post_url, "Failed", status[:80])
            stats["failed"] += 1
            # NO cooldown
            if stop_on_fail:
                break

    duration = _time.time() - run_start
    logger.section(
        f"POST MODE DONE — Posted:{stats['posted']}  "
        f"Skipped:{stats['skipped']}  Failed:{stats['failed']}"
    )
    return stats


# ════════════════════════════════════════════════════════════════════════════════
#  CREATE IMAGE POST
# ════════════════════════════════════════════════════════════════════════════════

def _create_image_post(driver, img_url: str, caption: str, logger: Logger) -> Dict:
    """
    Upload an image to DamaDam and publish it.

    Steps:
      1. Download image to temp file
      2. Navigate to /share/photo/upload/
      3. Send file path to file input
      4. Wait for upload to process
      5. Fill caption textarea
      6. Set radio options (never expire, replies off)
      7. Click submit
      8. Wait for redirect
      9. Determine result from final URL + page content
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

        # Check for immediate upload-denied (cooldown active before we even start)
        if "upload-denied" in cur or "denied" in cur:
            wait_s = _parse_countdown_seconds(driver.page_source)
            logger.warning(f"Upload-denied on page load — cooldown {wait_s}s active")
            return {"status": "Rate Limited", "url": driver.current_url, "wait_seconds": wait_s}

        # ── Step 3: File input ────────────────────────────────────────────────
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

        # Make hidden input interactable
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
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"Could not make file input visible: {e}")

        try:
            file_input.send_keys(abs_path)
            logger.info("File path sent to input")
        except Exception as e:
            logger.error(f"send_keys to file input failed: {e}")
            return {"status": f"File Input Error: {str(e)[:60]}", "url": driver.current_url}

        # ── Step 4: Wait for upload to process ────────────────────────────────
        logger.info("Waiting for upload to process (up to 10s)...")
        page_before = driver.page_source
        upload_settled = False
        for tick in range(10):
            time.sleep(1)
            try:
                page_now = driver.page_source
                if page_now != page_before:
                    logger.info(f"Page changed after {tick+1}s — upload detected")
                    upload_settled = True
                    time.sleep(1)
                    break
                page_before = page_now
            except Exception:
                pass

        if not upload_settled:
            logger.info("Page didn't change after file select — proceeding anyway")

        _dump(driver, logger, "02_after_file_select")

        # ── Step 5: Fill caption ───────────────────────────────────────────────
        clean_cap = sanitize_caption(strip_non_bmp(caption))
        cap_filled = _fill_textarea(driver, logger, clean_cap)
        if not cap_filled:
            logger.warning("Caption fill failed — posting without caption")

        # ── Step 6: Radio options ─────────────────────────────────────────────
        _click_radio_label(driver, logger, "exp-first", "Never expire post")
        _click_radio_label(driver, logger, "com-off",   "Turn Off Replies: Yes")
        _dump(driver, logger, "03_before_submit")

        # ── Step 7: Submit ────────────────────────────────────────────────────
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

        # ── Step 8: Wait for redirect ─────────────────────────────────────────
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

        # ── Step 9: Determine result ──────────────────────────────────────────

        # 9a. Duplicate image — redirect to /share/photo/upload-denied/
        #     Page shows: "Duplicate image!" heading
        if _detect_repeating_image(page_low) or "upload-denied" in final_url.lower():
            if _detect_caption_error(page_low):
                return {"status": "Caption Error", "url": final_url}
            return {"status": "Repeating", "url": final_url}

        # 9b. Caption error — also on upload-denied page
        #     Page shows: "Cannot share image!" + repeated-char message
        if _detect_caption_error(page_low):
            return {"status": "Caption Error", "url": final_url}

        # 9c. Rate limit / cooldown timer on page
        #     DamaDam shows a countdown: "1 min say pehlay post nahi kar saktay"
        #     We parse the seconds from the page so we can sleep exactly right
        wait_s = _detect_rate_limit(page_low)
        if wait_s:
            logger.warning(f"Rate limit detected — page says wait {wait_s}s")
            return {"status": "Rate Limited", "url": final_url, "wait_seconds": wait_s}

        # 9d. Success — redirected away from upload page
        if redirected and "upload" not in final_url.lower():
            post_url = _extract_post_url(driver)
            return {"status": "Posted", "url": post_url}

        # 9e. Still on upload/share page — something else went wrong
        if "upload" in final_url.lower() or "share" in final_url.lower():
            err = _extract_error_message(driver)
            logger.error(f"Still on upload page. Error: {err}")
            return {"status": f"Upload Error: {err}", "url": final_url}

        # 9f. Any other URL → assume success
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
#  CREATE TEXT POST
# ════════════════════════════════════════════════════════════════════════════════

def _create_text_post(driver, content: str, logger: Logger) -> Dict:
    """Create a text post on DamaDam."""
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
#  DETECTION HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _detect_repeating_image(page_source_lower: str) -> bool:
    """
    Detect duplicate-image rejection page.

    DamaDam redirects to /share/photo/upload-denied/ and shows:
      "Duplicate image!" (English heading)
      "Is jesa image pehle upload ho chuka hai" (body text)
      "Kuch new upload karein!" (CTA)
    """
    indicators = [
        "duplicate image",                      # heading (image 1 in screenshots)
        "pehle upload ho chuka",                # body text
        "kuch new upload karein",               # CTA
        "already posted",
        "dobara", "doosri baar",
    ]
    return any(ind in page_source_lower for ind in indicators)


def _detect_caption_error(page_source_lower: str) -> bool:
    """
    Detect caption-rejection page.

    DamaDam shows: "Cannot share image!"
    with body: "...itni zyada dafa aik hi character repeat nahi karein..."
    (image 3 in screenshots)
    """
    indicators = [
        "cannot share image",
        "itni zyada dafa aik hi character",
        "image ko sahi se describe karein",
        "wapis jaien aur dubara try karein",    # only on error pages, not success
    ]
    return any(ind in page_source_lower for ind in indicators)


def _detect_rate_limit(page_source_lower: str) -> int:
    """
    Detect DamaDam cooldown and parse exact seconds to wait.

    DamaDam shows a live countdown on the upload page:
      "Ap image share kar sakein ge 1 min baad..."   → 60s
      "2 min say pehlay post nahi kar saktay"         → 120s
      "23 sec baad share kar saktay hain"             → 23s

    Also strips <script> blocks first to avoid New Relic false positives.
    Returns seconds to wait, or 0 if no rate limit detected.
    """
    # Strip script blocks — New Relic JS contains rate-limit strings
    src = re.sub(r'<script[\s\S]*?</script>', '', page_source_lower, flags=re.IGNORECASE)

    # Try to parse exact seconds from countdown text
    # Pattern: "23 sec baad" or "1 min baad" or "2 min say pehlay"
    m = re.search(r'(\d+)\s*sec\b', src)
    if m:
        return max(int(m.group(1)) + 5, 30)   # +5 buffer, min 30s

    m = re.search(r'(\d+)\s*min\b', src)
    if m:
        return int(m.group(1)) * 60 + 10      # convert to seconds + 10s buffer

    # Keyword fallback
    for phrase in (
        "ap image share kar sakein ge",        # countdown message on upload page
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
    """
    Parse cooldown seconds from DamaDam's upload-denied page on initial load.
    Returns Config.POST_COOLDOWN_SECONDS if can't parse.
    """
    result = _detect_rate_limit(page_source.lower())
    return result if result else Config.POST_COOLDOWN_SECONDS


# ════════════════════════════════════════════════════════════════════════════════
#  FORM HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _fill_textarea(driver, logger: Logger, text: str) -> bool:
    """
    Find a textarea on the page and fill it.
    Tries confirmed DamaDam selectors first, then generic fallbacks.
    """
    selectors = [
        "textarea#pub_img_caption_field",   # confirmed DamaDam image upload page
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
                    # React fallback
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
    """
    Find the submit/share button on the DamaDam upload form.
    Confirmed selector: button#share_img_btn
    """
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

    # Text-based fallback
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
    """
    Click a DamaDam radio option by its <label for="..."> element.
    DamaDam uses hidden radio inputs (opacity:0); must click the label.
    """
    try:
        label = driver.find_element(By.CSS_SELECTOR, f"label[for='{label_for}']")
        driver.execute_script("arguments[0].click();", label)
        logger.info(f"Radio selected: {description} (label[for={label_for!r}])")
    except Exception as e:
        logger.warning(f"Could not select radio '{description}': {e}")


# ════════════════════════════════════════════════════════════════════════════════
#  POST URL EXTRACTION
# ════════════════════════════════════════════════════════════════════════════════

def _extract_post_url(driver) -> str:
    """
    Extract the newly created post URL from the page after redirect.

    DamaDam success redirect targets:
      /users/<nick>/                ← profile page (most common)
      /profile/public/<nick>/       ← alternate format
      /comments/image/<id>          ← direct post URL
    All count as success. We try to get the specific post URL; fall back to current URL.
    """
    # og:url meta tag (only on post pages)
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
#  CAPTION BUILDER
# ════════════════════════════════════════════════════════════════════════════════

def _build_caption(item: Dict) -> str:
    """
    Build the post caption from the URDU column (plain text only).

    URDU column workflow:
      - Rekhta mode writes a GOOGLETRANSLATE() formula initially
      - User manually replaces formula with actual Urdu text (paste as values)
      - Bot reads the plain text value — if it starts with '=' it's still a formula
        and was already filtered to "" before this point in run()

    Signature appended if DD_POST_SIGNATURE is set in .env.
    """
    parts = []
    urdu = (item.get("urdu") or "").strip()

    if urdu and not urdu.startswith("="):
        parts.append(urdu)

    if Config.POST_SIGNATURE:
        parts.append(Config.POST_SIGNATURE)

    return "\n".join(parts)


# ════════════════════════════════════════════════════════════════════════════════
#  POST LOG
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
