"""
modes/post.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Post Mode: Create new posts on DamaDam from PostQueue sheet.

What it does:
  1. Reads PostQueue sheet — all rows where STATUS = Pending
  2. Builds a duplicate index of all already-posted IMG_LINK values
     (batch loaded at start — NO per-row comparison to sheet)
  3. For each pending row:
     a. Skip if IMG_LINK already in duplicate index → mark Repeating
     b. Download image → upload to DamaDam /share/photo/upload/
     c. Set caption (URDU col), radio options (never expire, allow comments)
     d. Submit → detect rate limit (2min10s) or duplicate image rejection
     e. Write POST_URL and update STATUS
  4. On rate limit → wait the required time then retry ONCE
  5. On duplicate image detection → mark Repeating, NO retry

Rules enforced:
  - DamaDam cooldown: minimum 135 seconds between posts
  - Duplicate images: never attempt to post the same IMG_LINK twice
  - On Error or Repeating: mark the row and move on, never retry
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


def _dump_debug_artifacts(driver, logger: Logger, label: str) -> None:
    try:
        ts = time.strftime("%Y%m%d_%H%M%S")
        base = os.path.join(str(Config.LOG_DIR), f"post_debug_{ts}_{label}")
        png_path = base + ".png"
        html_path = base + ".html"

        try:
            driver.save_screenshot(png_path)
            logger.debug(f"Saved screenshot: {png_path}")
        except Exception as e:
            logger.debug(f"Screenshot failed: {e}")

        try:
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source or "")
            logger.debug(f"Saved HTML: {html_path}")
        except Exception as e:
            logger.debug(f"HTML dump failed: {e}")
    except Exception:
        pass


# ── DamaDam share page URLs ────────────────────────────────────────────────────
_URL_IMAGE_UPLOAD = f"{Config.BASE_URL}/share/photo/upload/"
_URL_TEXT_SHARE   = f"{Config.BASE_URL}/share/text/"

# ── Selectors for the share forms ─────────────────────────────────────────────
_SEL_FILE_INPUT  = "input[type='file'], input[name='file'], input[name='image']"
_SEL_CAPTION     = "textarea"
_SEL_TITLE_INPUT = "input[name='title'], #id_title"
_SEL_TAGS_INPUT  = "input[name='tags'], #id_tags"
_SEL_TEXT_AREA   = "textarea[name='text'], #id_text, textarea[name='content'], textarea"
_SEL_SUBMIT      = "button[type='submit'], input[type='submit'], button.btn-primary"


def _share_page_preflight(driver, logger: Logger, expected_path_contains: str) -> Optional[Dict]:
    """Validate we are on the expected share page and not redirected to login/denied."""
    try:
        cur = (driver.current_url or "").lower()
        if "login" in cur:
            if Config.DEBUG:
                _dump_debug_artifacts(driver, logger, "redirected_to_login")
            return {"status": "Login Required", "url": driver.current_url}
        if "denied" in cur or "access" in cur and "denied" in (driver.page_source or "").lower():
            if Config.DEBUG:
                _dump_debug_artifacts(driver, logger, "access_denied")
            return {"status": "Denied", "url": driver.current_url}
        if expected_path_contains and expected_path_contains not in cur:
            # Not a hard fail (site may redirect) but capture for debugging
            if Config.DEBUG:
                _dump_debug_artifacts(driver, logger, "unexpected_share_url")
        # CSRF token is often provided as hidden input; if missing, form post may fail
        try:
            csrf_inputs = driver.find_elements(By.CSS_SELECTOR, "input[name='csrfmiddlewaretoken']")
            if not csrf_inputs and Config.DEBUG:
                _dump_debug_artifacts(driver, logger, "csrf_missing")
        except Exception:
            pass
        return None
    except Exception:
        return None


def run(driver, sheets: SheetsManager, logger: Logger,
        max_posts: int = 0,
        stop_on_fail: bool = False,
        force_wait: int | None = None) -> Dict:
    """
    Run Post Mode end-to-end.

    Args:
        driver:    Selenium WebDriver (already logged in)
        sheets:    Connected SheetsManager
        logger:    Logger
        max_posts: 0 = process all Pending rows; N = stop after N

    Returns:
        Stats dict: {posted, skipped, failed, total}
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

    # ── Parse headers ─────────────────────────────────────────────────────────
    headers    = all_rows[0]
    header_map = SheetsManager.build_header_map(headers)

    # Resolve write-columns (1-based)
    col_status   = sheets.get_col(headers, "STATUS")
    col_post_url = sheets.get_col(headers, "POST_URL")
    col_notes    = sheets.get_col(headers, "NOTES")

    if not all([col_status, col_post_url, col_notes]):
        logger.error(f"PostQueue missing required columns. Found: {headers}")
        return {}

    def cell(row, *names):
        return SheetsManager.get_cell(row, header_map, *names)

    # ── Pre-run cooldown check (avoid immediate rate limit) ───────────────────
    if not Config.DRY_RUN:
        now = time.time()
        # Find the most recent Done row timestamp in PostQueue
        recent_done_ts = None
        if col_status and col_notes:
            for row in all_rows[1:]:
                if cell(row, "STATUS").lower() == "done":
                    notes = cell(row, "NOTES")
                    # Look for "Posted @ PKT" pattern in notes
                    m = re.search(r"Posted @ \d{2}-\w{3}-\d{2} \d{1,2}:\d{2}:\d{2} [AP]M", notes)
                    if m:
                        try:
                            ts_str = m.group(0).replace("Posted @ ", "")
                            dt = datetime.strptime(ts_str, "%d-%b-%y %I:%M:%S %p")
                            dt = dt.replace(tzinfo=PKT)
                            recent_done_ts = dt.timestamp()
                            break
                        except Exception:
                            continue
        if recent_done_ts:
            elapsed = now - recent_done_ts
            required = Config.POST_COOLDOWN_SECONDS
            if elapsed < required:
                wait = required - elapsed
                logger.info(f"Pre-run cooldown: waiting {wait:.0f}s before starting...")
                time.sleep(wait)

    # ── Force wait (manual override) ───────────────────────────────────────────
    if force_wait is not None and force_wait > 0 and not Config.DRY_RUN:
        logger.info(f"Force wait: waiting {force_wait}s before starting...")
        time.sleep(force_wait)

    # ── Build duplicate index (BATCH — all at once) ───────────────────────────
    # Load IMG_LINK values only from rows that are marked Done (successful posts).
    # This avoids treating Failed/Skipped rows as duplicates.
    col_img_link = sheets.get_col(headers, "IMG_LINK")
    posted_urls: Set[str] = set()
    if col_img_link:
        for row in all_rows[1:]:
            st = cell(row, "STATUS").lower()
            if st != "done":
                continue  # only truly successful posts count as duplicates
            img = cell(row, "IMG_LINK")
            if img:
                posted_urls.add(img.lower())
        logger.info(f"Duplicate index built: {len(posted_urls)} already-posted images")

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
        urdu     = cell(row, "URDU")
        title    = cell(row, "TITLE")
        poet     = cell(row, "POET")
        notes    = cell(row, "NOTES")

        # Duplicate image check
        if post_type == "image" and img_link and img_link.lower() in posted_urls:
            dup_count += 1
            sheets.update_row_cells(ws, i, {
                col_status: "Repeating",
                col_notes:  "Duplicate IMG_LINK",
            })
            continue

        pending.append({
            "row":       i,
            "type":      post_type,
            "img_link":  img_link,
            "urdu":      urdu,
            "title":     title,
            "poet":      poet,
        })

    if dup_count:
        logger.info(f"Skipped {dup_count} duplicate image rows")

    if not pending:
        logger.info("No Pending rows to post")
        return {"posted": 0, "skipped": 0, "failed": 0, "total": 0}

    if max_posts and max_posts > 0:
        pending = pending[:max_posts]

    logger.info(f"Found {len(pending)} rows to post")

    # ── Process each row ──────────────────────────────────────────────────────
    stats = {"posted": 0, "skipped": 0, "failed": 0, "total": len(pending)}
    last_post_time: float = 0.0   # tracks when the last post was successfully submitted

    for idx, item in enumerate(pending, start=1):
        row_num   = item["row"]
        post_type = item["type"]
        img_link  = item["img_link"]
        logger.info(f"[{idx}/{len(pending)}] Processing row {row_num} ({post_type})")

        # -- Enforce cooldown between posts ------------------------------------
        if last_post_time > 0:
            elapsed  = time.time() - last_post_time
            required = Config.POST_COOLDOWN_SECONDS
            if elapsed < required:
                wait = required - elapsed
                logger.info(f"Cooldown: waiting {wait:.0f}s before next post...")
                time.sleep(wait)

        # -- Build caption (Urdu text + optional signature) -------------------
        caption = _build_caption(item)

        # -- Create the post ---------------------------------------------------
        if post_type == "image":
            if not img_link:
                logger.skip(f"Row {row_num} — no IMG_LINK, skipping")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Skipped",
                    col_notes:  "No IMG_LINK",
                })
                stats["skipped"] += 1
                continue

            result = _create_image_post(driver, img_link, caption, logger)

        else:  # text
            content = item["urdu"] or item["title"]
            if not content:
                logger.skip(f"Row {row_num} — no text content, skipping")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Skipped",
                    col_notes:  "No content",
                })
                stats["skipped"] += 1
                continue
            result = _create_text_post(driver, content, logger)

        # -- Handle result ----------------------------------------------------
        status   = result.get("status", "Error")
        post_url = result.get("url", "")
        wait_s   = result.get("wait_seconds", 0)

        if status == "Posted":
            logger.ok(f"Post published: {post_url}")
            sheets.update_row_cells(ws, row_num, {
                col_status:   "Done",
                col_post_url: post_url,
                col_notes:    f"Posted @ {pkt_stamp()}",
            })
            sheets.log_action("POST", f"post_{post_type}", "", post_url, "Done")
            posted_urls.add((img_link or "").lower())  # Update runtime duplicate index
            last_post_time = time.time()
            stats["posted"] += 1

        elif status == "Dry Run":
            logger.info(f"Row {row_num} — dry run (not submitted)")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Skipped",
                col_notes:  "Dry run — not submitted",
            })
            stats["skipped"] += 1

        elif status == "Rate Limited":
            # DamaDam returned a rate limit.
            # If stop_on_fail is enabled, don't waste time waiting — stop immediately.
            if stop_on_fail:
                logger.warning("Rate limited — stop-on-fail enabled, leaving row Pending for later")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Pending (RateLimited)",
                    col_notes:  f"Rate limited @ {pkt_stamp()}",
                })
                stats["skipped"] += 1
                break

            # Otherwise: wait then retry ONCE.
            # Use whichever is LARGER: the page-detected wait, or 5 minutes minimum.
            # DamaDam's actual cooldown is often 5-10 minutes — 2 minutes is not enough.
            MIN_RATE_LIMIT_WAIT = 300  # 5 minutes minimum
            wait = max(wait_s or 0, Config.POST_COOLDOWN_SECONDS, MIN_RATE_LIMIT_WAIT) + 30
            logger.warning(f"Rate limited — waiting {wait}s ({wait//60:.0f}m {wait%60:.0f}s) then retrying once...")
            time.sleep(wait)
            if post_type == "image":
                result2 = _create_image_post(driver, img_link, caption, logger)
            else:
                result2 = _create_text_post(driver, item["urdu"] or item["title"], logger)

            if result2.get("status") == "Posted":
                logger.ok(f"Retry succeeded: {result2['url']}")
                sheets.update_row_cells(ws, row_num, {
                    col_status:   "Done",
                    col_post_url: result2["url"],
                    col_notes:    f"Posted (retry) @ {pkt_stamp()}",
                })
                last_post_time = time.time()
                stats["posted"] += 1
            else:
                st2 = result2.get("status", "Error")
                if st2 == "Rate Limited":
                    # Still rate limited after the extended wait — DamaDam is heavily throttling.
                    # Leave the row as Pending and stop all further processing this run.
                    # Do NOT mark as Failed — it will be retried next time the bot runs.
                    logger.warning("Still rate limited after retry — DamaDam is throttling heavily. Stopping this run.")
                    sheets.update_row_cells(ws, row_num, {
                        col_status: "Pending (RateLimited)",
                        col_notes:  f"Rate limited twice @ {pkt_stamp()}",
                    })
                    stats["skipped"] += 1
                    break  # Stop all further posts — site is throttled, no point continuing
                else:
                    logger.error(f"Retry also failed: {st2}")
                    sheets.update_row_cells(ws, row_num, {
                        col_status: "Failed",
                        col_notes:  f"Rate limited, retry: {st2}",
                    })
                    stats["failed"] += 1
                    if stop_on_fail:
                        logger.warning("Stop-on-fail enabled — stopping after failure")
                        break

        elif status == "Repeating":
            # DamaDam detected this as a duplicate image — do NOT retry
            logger.warning(f"Row {row_num} — DamaDam duplicate image rejection")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Repeating",
                col_notes:  "DamaDam rejected: duplicate image",
            })
            stats["skipped"] += 1

        else:
            # Any other error — mark failed, do NOT retry
            logger.error(f"Row {row_num} failed: {status}")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Failed",
                col_notes:  status[:80],
            })
            sheets.log_action("POST", f"post_{post_type}", "", post_url, "Failed", status)
            stats["failed"] += 1
            if stop_on_fail:
                logger.warning("Stop-on-fail enabled — stopping after failure")
                break

    duration = _time.time() - run_start
    logger.section(
        f"POST MODE DONE — Posted:{stats['posted']}  "
        f"Skipped:{stats['skipped']}  Failed:{stats['failed']}"
    )
    sheets.log_run(
        "post",
        {"posted": stats["posted"], "failed": stats["failed"], "skipped": stats["skipped"]},
        duration_s=duration,
        notes=f"{stats['posted']}/{stats['total']} posts published",
    )
    return stats


# ════════════════════════════════════════════════════════════════════════════════
#  CREATE IMAGE POST
# ════════════════════════════════════════════════════════════════════════════════

def _create_image_post(driver, img_url: str, caption: str,
                       logger: Logger) -> Dict:
    """
    Download an image from img_url and upload it to DamaDam.

    DamaDam image upload flow:
      1. GET /share/photo/upload/
      2. The page loads a form with a file input and a caption textarea
      3. After file is selected, DamaDam shows a PREVIEW before the textarea
         becomes active — we must wait for the preview to appear
      4. Fill caption AFTER preview loads (not before)
      5. Set radio buttons (exp=i, com=0)
      6. Submit and wait for redirect to /comments/image/{id}

    Returns:
        {"status": "Posted"|"Rate Limited"|"Repeating"|"Error: ...", "url": "..."}
    """
    tmp_path = ""
    try:
        # -- Download image to temp file --------------------------------------
        logger.debug(f"Downloading image: {img_url}")
        tmp_path = download_image(img_url, logger)

        # -- Open upload page -------------------------------------------------
        driver.get(_URL_IMAGE_UPLOAD)
        time.sleep(1)
        pre = _share_page_preflight(driver, logger, "/share/photo/upload")
        if pre:
            return pre
        # Wait for the page to fully load — specifically for the file input
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
            )
        except TimeoutException:
            pass
        time.sleep(2)

        # -- Find the file input (search whole page, not inside form) ---------
        # DamaDam's file input is sometimes outside a <form> tag or uses JS handling.
        # We search the full page rather than inside a form element.
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
                break

        if not file_input:
            logger.warning("File input not found on upload page")
            return {"status": "Form Error: no file input", "url": ""}

        # -- Send the file path to the input ----------------------------------
        abs_path = os.path.abspath(tmp_path)
        logger.debug(f"Uploading: {abs_path}")
        file_input.send_keys(abs_path)

        # -- Wait for upload preview to appear --------------------------------
        # DamaDam shows a thumbnail/preview after the file is selected.
        # The caption textarea only becomes properly interactive AFTER this.
        # We wait up to 15s for the preview image or a hidden-state change.
        preview_appeared = False
        for _ in range(15):
            time.sleep(1)
            try:
                # Preview appears as an img tag or a div with background-image
                previews = driver.find_elements(
                    By.CSS_SELECTOR,
                    "img.uploadPreview, div.uploadPreview, img[src*='blob:'], "
                    "img[src*='data:image'], .preview img, #preview img, "
                    "img[id*='preview'], img[class*='preview']"
                )
                if previews:
                    preview_appeared = True
                    break
                # Fallback: check if the file input value is set
                val = (file_input.get_attribute("value") or "").strip()
                if val:
                    preview_appeared = True
                    break
            except Exception:
                pass

        if not preview_appeared:
            # Still try — some DamaDam versions don't show a visible preview
            logger.debug("Preview not confirmed — proceeding anyway after wait")
        time.sleep(2)

        # -- Fill caption AFTER preview loads ---------------------------------
        clean_cap = sanitize_caption(strip_non_bmp(caption))
        if clean_cap:
            # The caption textarea may be inside or outside the upload form
            # Try multiple selectors targeting common DamaDam textarea names
            cap_filled = False
            for sel in (
                "textarea[name='description']",
                "textarea[name='caption']",
                "textarea[name='text']",
                "textarea[name='body']",
                "textarea",
            ):
                try:
                    areas = driver.find_elements(By.CSS_SELECTOR, sel)
                    if areas:
                        areas[0].clear()
                        time.sleep(0.3)
                        areas[0].send_keys(clean_cap)
                        cap_filled = True
                        logger.debug(f"Caption filled ({len(clean_cap)} chars) via {sel}")
                        break
                except Exception:
                    continue
            if not cap_filled:
                logger.warning("Could not find caption textarea — posting without caption")

        # -- Set post options -------------------------------------------------
        # exp=i → Never expire  |  com=0 → Allow comments
        # These radios are searched page-wide (not form-scoped)
        for name, value in (("exp", "i"), ("com", "0")):
            try:
                radio = driver.find_element(
                    By.CSS_SELECTOR,
                    f"input[type='radio'][name='{name}'][value='{value}']"
                )
                if not radio.is_selected():
                    driver.execute_script("arguments[0].click();", radio)
            except Exception:
                pass

        # -- Find and click submit --------------------------------------------
        submit = None
        for sel in (
            "button[type='submit'][name='dec'][value='1']",
            "input[type='submit'][name='dec'][value='1']",
            "button[type='submit']",
            "input[type='submit']",
        ):
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                submit = els[0]
                break

        if not submit:
            return {"status": "Form Error: no submit button", "url": ""}

        if Config.DRY_RUN:
            logger.debug("DRY_RUN enabled — not submitting. Dumping artifacts...")
            _dump_debug_artifacts(driver, logger, "before_submit_image")
            return {"status": "Dry Run", "url": driver.current_url}

        logger.debug("Submitting image post...")
        driver.execute_script("arguments[0].click();", submit)

        # -- Wait for redirect after submit -----------------------------------
        # DamaDam redirects to /comments/image/{id} on success
        try:
            WebDriverWait(driver, 20).until(
                lambda d: (
                    "/comments/image/" in d.current_url
                    or "/content/" in d.current_url
                    or (d.current_url != _URL_IMAGE_UPLOAD
                        and "upload" not in d.current_url.lower())
                )
            )
        except TimeoutException:
            pass
        time.sleep(2)

        # -- Detect rate limit or duplicate -----------------------------------
        page = driver.page_source.lower()

        wait_s = _detect_rate_limit(page)
        if wait_s:
            return {"status": "Rate Limited", "url": driver.current_url, "wait_seconds": wait_s}

        if _detect_repeating_image(page):
            return {"status": "Repeating", "url": driver.current_url}

        # -- Extract posted URL -----------------------------------------------
        post_url = _extract_post_url(driver)
        if is_share_or_denied_url(post_url):
            return {"status": "Denied", "url": post_url}

        if "/comments/image/" in post_url or "/content/" in post_url:
            return {"status": "Posted", "url": post_url}

        # If URL still looks like an upload page, check for error messages
        if "upload" in driver.current_url.lower():
            err_text = _extract_error_message(driver)
            if Config.DEBUG:
                _dump_debug_artifacts(driver, logger, "upload_error_image")
            return {"status": f"Upload Error: {err_text}", "url": driver.current_url}

        return {"status": "Pending Verification", "url": post_url}

    except RuntimeError as e:
        return {"status": f"Image Download Failed: {str(e)[:60]}", "url": ""}
    except Exception as e:
        if Config.DEBUG:
            _dump_debug_artifacts(driver, logger, "exception_image")
        return {"status": f"Error: {str(e)[:60]}", "url": ""}
    finally:
        # Always clean up the temp file regardless of outcome
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════════
#  CREATE TEXT POST
# ════════════════════════════════════════════════════════════════════════════════

def _create_text_post(driver, content: str, logger: Logger) -> Dict:
    """
    Create a text post on DamaDam.

    Returns:
        {"status": "Posted"|"Rate Limited"|"Error: ...", "url": "..."}
    """
    try:
        driver.get(_URL_TEXT_SHARE)
        time.sleep(1)
        pre = _share_page_preflight(driver, logger, "/share/text")
        if pre:
            return pre
        time.sleep(3)

        form = _find_share_form(driver, require_file=False)
        if not form:
            return {"status": "Form Error", "url": ""}

        # -- Fill text content ------------------------------------------------
        clean_content = sanitize_caption(strip_non_bmp(content))
        try:
            text_area = form.find_element(By.CSS_SELECTOR, _SEL_TEXT_AREA)
            text_area.clear()
            text_area.send_keys(clean_content)
        except Exception as e:
            return {"status": f"Textarea Error: {str(e)[:40]}", "url": ""}

        # -- Set post options -------------------------------------------------
        _set_radio(driver, form, "exp", "i")
        _set_radio(driver, form, "com", "0")

        # -- Submit -----------------------------------------------------------
        submit = form.find_element(By.CSS_SELECTOR, _SEL_SUBMIT)
        if Config.DRY_RUN:
            logger.debug("DRY_RUN enabled — not submitting. Dumping artifacts...")
            _dump_debug_artifacts(driver, logger, "before_submit_text")
            return {"status": "Dry Run", "url": driver.current_url}

        driver.execute_script("arguments[0].click();", submit)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.current_url != _URL_TEXT_SHARE
            )
        except TimeoutException:
            pass
        time.sleep(2)

        page = driver.page_source.lower()

        wait_s = _detect_rate_limit(page)
        if wait_s:
            return {"status": "Rate Limited", "url": driver.current_url, "wait_seconds": wait_s}

        post_url = _extract_post_url(driver)
        if is_share_or_denied_url(post_url):
            return {"status": "Denied", "url": post_url}

        if "/comments/text/" in post_url:
            return {"status": "Posted", "url": post_url}

        return {"status": "Pending Verification", "url": post_url}

    except Exception as e:
        if Config.DEBUG:
            _dump_debug_artifacts(driver, logger, "exception_text")
        return {"status": f"Error: {str(e)[:60]}", "url": ""}


# ════════════════════════════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════════════════════════════

def _find_share_form(driver, require_file: bool = False):
    """
    Find the share/upload form on the current page.
    Searches page-wide (not just inside <form> tags) because DamaDam
    sometimes uses JS-handled forms that don't wrap inputs properly.
    """
    try:
        forms = driver.find_elements(By.CSS_SELECTOR, "form")
        for form in forms:
            try:
                if require_file:
                    form.find_element(By.CSS_SELECTOR, "input[type='file']")
                else:
                    form.find_element(By.CSS_SELECTOR, "textarea")
                return form
            except Exception:
                continue
        return None
    except Exception:
        return None


def _extract_error_message(driver) -> str:
    """
    Try to find and return a visible error message on the current page.
    Used when the upload page doesn't redirect — helps with debugging.
    """
    for sel in (
        ".errorlist li", ".alert-danger", ".error", "div.err",
        "p.error", "span.error", ".messages li",
    ):
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                text = (els[0].text or "").strip()
                if text:
                    return text[:80]
        except Exception:
            pass
    return "unknown error — check page manually"


def _set_radio(driver, form, name: str, value: str):
    """
    Select a radio button by name+value inside a form.
    Used for: exp (expiry) and com (comments allowed) options.
    Falls back to clicking by CSS id patterns if value selection fails.
    """
    try:
        radio = form.find_element(
            By.CSS_SELECTOR, f"input[type='radio'][name='{name}'][value='{value}']"
        )
        if not radio.is_selected():
            driver.execute_script("arguments[0].click();", radio)
    except Exception:
        pass


def _detect_rate_limit(page_source: str) -> int:
    """
    Check page source for DamaDam's rate limit message.
    Returns estimated wait time in seconds, or 0 if no rate limit.

    DamaDam may express rate limits in several ways:
      - English: "wait 2 minutes", "please wait", "too many requests"
      - Urdu/Roman-Urdu: "intezaar karein", "bahut zyada"
      - HTTP 429 text in page body
      - Generic throttle keywords
    """
    src = page_source.lower()

    # ── Timed messages: "2 minute" or "130 second" ───────────────────────────
    if "minute" in src and ("wait" in src or "please" in src or "karo" in src or "intezaar" in src):
        m = re.search(r"(\d+)\s*minute", src)
        if m:
            return int(m.group(1)) * 60 + 15  # add buffer
        return Config.POST_COOLDOWN_SECONDS

    if "second" in src and ("wait" in src or "please" in src or "ruko" in src):
        m = re.search(r"(\d+)\s*second", src)
        if m:
            return int(m.group(1)) + 10
        return Config.POST_COOLDOWN_SECONDS

    # ── Generic rate-limit / throttle keywords ────────────────────────────────
    rate_limit_phrases = [
        "too many",           # "too many requests" / "too many posts"
        "rate limit",         # explicit rate limit message
        "429",                # HTTP 429 in page body
        "slow down",          # "please slow down"
        "throttl",            # "throttled" / "throttling"
        "bahut zyada",        # Urdu: "too many"
        "intezaar",           # Urdu: "wait" (intezaar karein)
        "ruko",               # Urdu: "stop/wait"
        "zyada post",         # Urdu: "too many posts"
        "limit exceeded",     # generic limit message
        "try again",          # "please try again later"
    ]
    for phrase in rate_limit_phrases:
        if phrase in src:
            return Config.POST_COOLDOWN_SECONDS

    return 0


def _detect_repeating_image(page_source: str) -> bool:
    """
    Check if DamaDam rejected the image as a duplicate.
    Returns True if duplicate image indicators are found.
    """
    indicators = [
        "repeat", "already posted", "duplicate",
        "dobara", "doosri baar",  # Urdu/Roman Urdu variants
        "phir se",
    ]
    src = page_source.lower()
    return any(ind in src for ind in indicators)


def _extract_post_url(driver) -> str:
    """
    Try multiple strategies to extract the newly created post URL.
    Returns a clean URL string.
    """
    # Strategy 1: og:url meta tag (most reliable for redirect targets)
    try:
        og = driver.find_elements(By.CSS_SELECTOR, "meta[property='og:url']")
        if og:
            href = (og[0].get_attribute("content") or "").strip()
            if href and "/comments/" in href:
                return clean_post_url(href)
    except Exception:
        pass

    # Strategy 2: current URL (if DamaDam redirected after submit)
    current = clean_post_url(driver.current_url)
    if not is_share_or_denied_url(current):
        return current

    # Strategy 3: find any /comments/ link in the page
    try:
        links = driver.find_elements(
            By.CSS_SELECTOR,
            "a[href*='/comments/text/'], a[href*='/comments/image/'], a[href*='/content/']"
        )
        for a in links:
            href = (a.get_attribute("href") or "").strip()
            if href and "damadam.pk" in href:
                return clean_post_url(href)
    except Exception:
        pass

    # Strategy 4: regex search in page source
    try:
        m = re.search(
            r"https?://[^\s\"']*(/comments/(?:text|image)/\d+|/content/\d+)",
            driver.page_source
        )
        if m:
            return clean_post_url(m.group(0))
    except Exception:
        pass

    return clean_post_url(driver.current_url)


def _build_caption(item: Dict) -> str:
    """
    Build the final caption string for a post:
      [URDU lines]
      [Signature from Config]
    """
    parts = []
    urdu  = (item.get("urdu") or "").strip()
    if urdu:
        parts.append(urdu)
    if Config.POST_SIGNATURE:
        parts.append(Config.POST_SIGNATURE)
    return "\n".join(parts)
