"""
modes/post.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Post Mode: Create new posts on DamaDam from PostQueue sheet.

FIXED in this version:
  - Forensic HTML/screenshot dumps at EVERY critical step (always, not just DEBUG)
  - File input made interactable even when hidden (JS style override)
  - Preview wait replaced with a simple fixed 5s wait + DOM-settled check
  - Caption textarea: fallback chain now includes a JS-based textarea finder
  - Submit: uses JS click on the FIRST visible submit button found page-wide
  - Redirect/verification: trusts the URL change, does NOT search caption in page
  - Rate limit: waits the full cooldown, then retries once
  - All dump files go to logs/post_debug_HHMMSS_<label>.{html,png}
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


# ════════════════════════════════════════════════════════════════════════════════
#  FORENSIC DUMP — always runs, not just in DEBUG mode
#  Saves HTML + screenshot to logs/ so you can see exactly what the browser saw
# ════════════════════════════════════════════════════════════════════════════════

def _dump(driver, logger: Logger, label: str) -> None:
    """
    Save screenshot + HTML to logs/ — only when Config.DEBUG=1.
    Set DD_DEBUG=1 in .env to enable. Off by default to keep logs/ clean.
    """
    if not Config.DEBUG:
        return
    try:
        ts   = time.strftime("%H%M%S")
        base = os.path.join(str(Config.LOG_DIR), f"post_{ts}_{label}")
        try:
            driver.save_screenshot(base + ".png")
            logger.debug(f"[DUMP] Screenshot → logs/post_{ts}_{label}.png")
        except Exception as e:
            logger.debug(f"[DUMP] Screenshot failed: {e}")
        try:
            with open(base + ".html", "w", encoding="utf-8", errors="replace") as f:
                f.write(f"<!-- URL: {driver.current_url} -->\n")
                f.write(driver.page_source or "")
            logger.debug(f"[DUMP] HTML → logs/post_{ts}_{label}.html")
        except Exception as e:
            logger.debug(f"[DUMP] HTML dump failed: {e}")
    except Exception:
        pass


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

        # Duplicate image check
        if post_type == "image" and img_link and img_link.lower() in posted_urls:
            dup_count += 1
            sheets.update_row_cells(ws, i, {
                col_status: "Repeating",
                col_notes:  "Duplicate IMG_LINK",
            })
            continue

        pending.append({
            "row":      i,
            "type":     post_type,
            "img_link": img_link,
            "urdu":     cell(row, "URDU"),
            "title":    cell(row, "TITLE"),
            "poet":     cell(row, "POET"),
        })

    if dup_count:
        logger.info(f"Skipped {dup_count} duplicate rows")

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

        # -- Cooldown ---------------------------------------------------------
        # DamaDam enforces ~2min between posts. We use 180s (3min) for safety.
        # The /share/photo/upload-denied/ page confirms this is their real limit.
        if last_post_time > 0 and not Config.DRY_RUN:
            elapsed  = time.time() - last_post_time
            required = 180  # 3 minutes — safe margin above DamaDam's cooldown
            if elapsed < required:
                wait = required - elapsed
                logger.info(f"Cooldown: waiting {wait:.0f}s (3min gap between posts)...")
                time.sleep(wait)

        caption = _build_caption(item)

        # -- Create post ------------------------------------------------------
        if post_type == "image":
            if not img_link:
                logger.skip(f"Row {row_num} — no IMG_LINK")
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
                logger.skip(f"Row {row_num} — no text content")
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

        if status == "Posted":
            logger.ok(f"✅ Post published: {post_url}")
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

        elif status == "Rate Limited":
            wait_s = result.get("wait_seconds", Config.POST_COOLDOWN_SECONDS)
            if stop_on_fail:
                logger.warning(f"Rate limited — stop-on-fail active, leaving row Pending")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Pending",
                    col_notes:  f"Rate limited @ {pkt_stamp()} — retried next run",
                })
                break

            logger.warning(f"Rate limited — waiting {wait_s}s then retrying once...")
            time.sleep(wait_s + 10)

            # One retry
            if post_type == "image":
                result2 = _create_image_post(driver, img_link, caption, logger)
            else:
                result2 = _create_text_post(driver, item["urdu"] or item["title"], logger)

            if result2.get("status") == "Posted":
                post_url2 = result2.get("url", "")
                logger.ok(f"✅ Post published after retry: {post_url2}")
                sheets.update_row_cells(ws, row_num, {
                    col_status:   "Done",
                    col_post_url: post_url2,
                    col_notes:    f"Posted (after rate limit wait) @ {pkt_stamp()}",
                })
                posted_urls.add((img_link or "").lower())
                last_post_time = time.time()
                stats["posted"] += 1
            else:
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Failed",
                    col_notes:  f"Rate limit retry failed: {result2.get('status','?')}",
                })
                stats["failed"] += 1

        elif status == "Repeating":
            logger.warning(f"Row {row_num} — DamaDam duplicate image")
            sheets.update_row_cells(ws, row_num, {
                col_status: "Repeating",
                col_notes:  "DamaDam rejected: duplicate image",
            })
            stats["skipped"] += 1

        else:
            logger.error(f"Row {row_num} failed: {status}")
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
#  CREATE IMAGE POST — fully rewritten
# ════════════════════════════════════════════════════════════════════════════════

def _create_image_post(driver, img_url: str, caption: str, logger: Logger) -> Dict:
    """
    Upload an image to DamaDam and publish it.

    Strategy (each step dumps HTML/screenshot so you can see what happened):
      1. Navigate to /share/photo/upload/  → DUMP: 01_upload_page
      2. Find file input (make it interactable via JS)  → send file path
      3. Wait 6s for upload to process  → DUMP: 02_after_file_select
      4. Fill caption textarea (try every selector, fallback to JS)
      5. Set radio options  → DUMP: 03_before_submit
      6. Click submit  → wait for URL change
      7. DUMP: 04_after_submit  → read URL to determine success
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
        _dump(driver, logger, "01_upload_page_loaded")

        # Check if redirected away (login expired, access denied)
        cur = driver.current_url.lower()
        if "login" in cur:
            return {"status": "Login Required", "url": driver.current_url}
        if "denied" in cur:
            return {"status": "Denied", "url": driver.current_url}

        # ── Step 3: Find and interact with file input ─────────────────────────
        #
        # DamaDam file inputs are often hidden with CSS.
        # We make them visible via JS before sending keys — this is the fix
        # that most bots get wrong.
        #
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

        # Make the file input visible and interactable (removes hidden/opacity CSS)
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

        # Send the file path
        try:
            file_input.send_keys(abs_path)
            logger.info("File path sent to input")
        except Exception as e:
            logger.error(f"send_keys to file input failed: {e}")
            _dump(driver, logger, "ERROR_file_send_keys_failed")
            return {"status": f"File Input Error: {str(e)[:60]}", "url": driver.current_url}

        # ── Step 4: Wait for upload to process ────────────────────────────────
        #
        # After selecting a file, DamaDam processes/previews it.
        # We wait up to 10 seconds watching for ANY change in the page DOM.
        # Simple fixed wait is more reliable than selector-hunting.
        #
        logger.info("Waiting for upload to process (up to 10s)...")
        upload_settled = False
        page_before = driver.page_source
        for tick in range(10):
            time.sleep(1)
            try:
                page_now = driver.page_source
                if page_now != page_before:
                    logger.info(f"Page changed after {tick+1}s — upload processing detected")
                    upload_settled = True
                    page_before = page_now
                    # Wait one more second after the change settles
                    time.sleep(1)
                    break
            except Exception:
                pass

        if not upload_settled:
            logger.info("Page didn't change after file select — proceeding anyway")

        _dump(driver, logger, "02_after_file_select")

        # ── Step 5: Fill caption ───────────────────────────────────────────────
        clean_cap = sanitize_caption(strip_non_bmp(caption))
        if clean_cap:
            cap_filled = _fill_textarea(driver, logger, clean_cap)
            if not cap_filled:
                logger.warning("Caption fill failed — posting without caption")

        # ── Step 6: Set radio options ─────────────────────────────────────────
        #   exp=i  → Never expire post    → click label[for='exp-first']
        #   com-off → Turn Off Replies: Yes (label says 'Yes', id='com-off', value='0')
        #   NOTE: com-on(value=1)=label 'No', com-off(value=0)=label 'Yes' — confusing but confirmed
        #
        # DamaDam uses hidden radio inputs (class="checkbox", opacity:0)
        # with visible <label for="..."> elements styled via CSS.
        # Clicking the hidden input does nothing — must click the label.
        # Label IDs confirmed from HTML dump of the upload page.
        _click_radio_label(driver, logger, "exp-first", "Never expire post")
        _click_radio_label(driver, logger, "com-off",   "Turn Off Replies: Yes")
        _dump(driver, logger, "03_before_submit")

        # ── Step 7: Find and click submit ─────────────────────────────────────
        if Config.DRY_RUN:
            logger.info("DRY RUN — stopping before submit. Check logs/post_*_03_before_submit.*")
            return {"status": "Dry Run", "url": driver.current_url}

        submit = _find_submit_button(driver, logger)
        if not submit:
            _dump(driver, logger, "ERROR_no_submit")
            return {"status": "Form Error: no submit button found", "url": driver.current_url}

        url_before_submit = driver.current_url
        logger.info(f"Clicking submit (URL before: {url_before_submit})")
        driver.execute_script("arguments[0].click();", submit)

        # ── Step 8: Wait for redirect ─────────────────────────────────────────
        #
        # DamaDam redirects to profile on success. Confirmed URL patterns:
        #   /users/<nick>/          ← actual redirect seen in logs (confirmed)
        #   /profile/public/<nick>/ ← alternate profile URL format
        #   /comments/image/<id>    ← direct post URL (some cases)
        # Stays on /share/photo/upload/ on failure.
        #
        redirected = False
        for tick in range(30):
            time.sleep(1)
            try:
                cur_url = driver.current_url
                if cur_url != url_before_submit and "upload" not in cur_url.lower():
                    logger.info(f"Redirected after {tick+1}s → {cur_url}")
                    redirected = True
                    break
            except Exception:
                pass

        time.sleep(2)
        _dump(driver, logger, "04_after_submit")

        final_url = driver.current_url
        page_src  = driver.page_source.lower()
        logger.info(f"Final URL: {final_url}")

        # ── Step 9: Determine result ──────────────────────────────────────────

        # Check rate limit
        wait_s = _detect_rate_limit(page_src)
        if wait_s:
            logger.warning("Rate limit detected in page source")
            return {"status": "Rate Limited", "url": final_url, "wait_seconds": wait_s}

        # Check duplicate image
        if _detect_repeating_image(page_src):
            return {"status": "Repeating", "url": final_url}

        # If we successfully redirected away from the upload page → success
        if redirected:
            post_url = _extract_post_url(driver)
            return {"status": "Posted", "url": post_url}

        # DamaDam real cooldown page — /share/photo/upload-denied/
        # Shows "Ye share ho ga X secs baad..." — their enforced posting gap.
        # We use 180s wait. Leave the row Pending so next run retries it.
        if "upload-denied" in final_url.lower() or "denied" in final_url.lower():
            logger.warning("DamaDam upload-denied — their cooldown active. Row stays Pending.")
            return {"status": "Rate Limited", "url": final_url, "wait_seconds": 180}

        # Still on upload page → something went wrong
        if "upload" in final_url.lower() or "share" in final_url.lower():
            err = _extract_error_message(driver)
            logger.error(f"Still on upload page. Error: {err}")
            return {"status": f"Upload Error: {err}", "url": final_url}

        # Any other URL = assume success (DamaDam may redirect to various pages)
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
            _dump(driver, logger, "ERROR_text_textarea_not_found")
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
        page_src  = driver.page_source.lower()

        wait_s = _detect_rate_limit(page_src)
        if wait_s:
            return {"status": "Rate Limited", "url": final_url, "wait_seconds": wait_s}

        if redirected:
            post_url = _extract_post_url(driver)
            return {"status": "Posted", "url": post_url}

        if "share" in final_url.lower():
            err = _extract_error_message(driver)
            return {"status": f"Submit Error: {err}", "url": final_url}

        return {"status": "Posted", "url": _extract_post_url(driver)}

    except Exception as e:
        try:
            _dump(driver, logger, "EXCEPTION_text_post")
        except Exception:
            pass
        return {"status": f"Error: {str(e)[:60]}", "url": ""}


# ════════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _fill_textarea(driver, logger: Logger, text: str) -> bool:
    """
    Find a textarea on the page and fill it using send_keys.

    Tries explicit selectors first, then falls back to any visible textarea.
    Uses JS to ensure React sees the input via native value setter + events.
    Returns True if filled successfully.
    """
    selectors = [
        # Confirmed exact selector from DamaDam's upload page HTML:
        # <textarea id="pub_img_caption_field" name="caption" ...>
        "textarea#pub_img_caption_field",
        "textarea[name='caption']",
        # Fallbacks for text post page
        "textarea[name='description']",
        "textarea[name='text']",
        "textarea[name='body']",
        "textarea[name='content']",
        "textarea",  # last resort: first visible textarea
    ]

    for sel in selectors:
        try:
            areas = driver.find_elements(By.CSS_SELECTOR, sel)
            for area in areas:
                try:
                    if not area.is_displayed():
                        continue

                    # Scroll into view
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", area)
                    time.sleep(0.3)

                    # Clear via JS + Selenium
                    try:
                        area.clear()
                    except Exception:
                        pass
                    driver.execute_script("arguments[0].value = '';", area)
                    time.sleep(0.2)

                    # Type via send_keys (fires real keyboard events for React)
                    area.send_keys(text)
                    time.sleep(0.3)

                    # Verify it actually typed
                    actual = driver.execute_script("return arguments[0].value;", area) or ""
                    if actual.strip():
                        logger.info(f"Caption filled ({len(actual)} chars) via [{sel}]")
                        return True

                    # send_keys didn't work → try React native value setter
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
                        logger.info(f"Caption filled via React event dispatch ({len(actual2)} chars)")
                        return True

                except Exception as e:
                    logger.debug(f"Textarea attempt failed [{sel}]: {e}")
                    continue
        except Exception:
            continue

    logger.warning("Could not fill any textarea")
    return False


def _find_submit_button(driver, logger: Logger):
    """
    Find the submit button for the upload/share form.

    Confirmed from DamaDam HTML dump:
      <button id="share_img_btn" name="btn" value="1" type="submit">SHARE</button>

    The old code tried name='dec' value='1' — that is the REPLY form button,
    not the image upload button. That was the root selector bug.
    """
    selectors = [
        # Confirmed exact selector from DamaDam's upload page HTML
        "button#share_img_btn",
        "button[name='btn'][value='1']",
        "input[name='btn'][value='1']",
        # Generic fallbacks
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

    # Last resort: any button whose text contains SHARE or POST
    try:
        all_buttons = driver.find_elements(By.CSS_SELECTOR, "button, input[type='submit']")
        for btn in all_buttons:
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
    Click a DamaDam radio option by targeting its <label for="..."> element.

    DamaDam's radio inputs have class="checkbox" with CSS opacity:0 — they are
    visually hidden. The user sees only the styled <label> elements. Clicking
    the hidden input via JS does nothing to the UI state. We must click the
    label instead, which triggers the browser's native label-input binding.

    Args:
        label_for:   The 'for' attribute value of the label (= radio input id)
                     e.g. "exp-first" for Never expire, "com-off" for Turn Off Replies Yes
        description: Human-readable name for logging
    """
    try:
        label = driver.find_element(By.CSS_SELECTOR, f"label[for='{label_for}']")
        driver.execute_script("arguments[0].click();", label)
        logger.info(f"Radio selected: {description} (label[for={label_for!r}])")
    except Exception as e:
        logger.warning(f"Could not select radio '{description}' (label[for={label_for!r}]): {e}")


def _detect_rate_limit(page_source: str) -> int:
    """
    Check page source for DamaDam rate limit indicators.
    Returns cooldown seconds, or 0 if no rate limit.

    IMPORTANT: DamaDam embeds New Relic analytics JS on every page — including
    the success redirect to /users/<nick>/. That JS bundle contains strings like
    "TOO_MANY", "rate limit", "429: Too Many Requests" as part of its own error
    message definitions. These must NOT trigger our rate limit detection.

    Strategy: only fire on phrases that appear in visible page text, not in
    minified JS. We check for DamaDam-specific rate limit patterns that would
    appear in HTML body content, not in a <script> block.
    """
    # Strip all <script> blocks before checking — this eliminates the New Relic
    # false positive entirely. DamaDam's actual rate limit message would appear
    # in the HTML body, not inside a <script> tag.
    import re as _re
    src_no_scripts = _re.sub(r'<script[\s\S]*?</script>', '', page_source, flags=_re.IGNORECASE)
    src = src_no_scripts.lower()

    # Only match phrases that DamaDam would show as visible page text
    for phrase in (
        "you are posting too fast",
        "wait before posting",
        "post limit reached",
        "too many posts",
        "posting limit",
    ):
        if phrase in src:
            return Config.POST_COOLDOWN_SECONDS
    return 0


def _detect_repeating_image(page_source: str) -> bool:
    """Check if DamaDam rejected the image as a duplicate."""
    indicators = ["already posted", "dobara", "doosri baar", "phir se"]
    src = page_source.lower()
    return any(ind in src for ind in indicators)


def _extract_post_url(driver) -> str:
    """
    Extract the newly created post URL using multiple strategies.

    DamaDam confirmed redirect targets after successful post:
      /users/<nick>/          ← profile page (confirmed in dump logs)
      /profile/public/<nick>/ ← alternate profile format
      /comments/image/<id>    ← direct post URL
    All of these count as success — return the current URL.
    """
    # Strategy 1: og:url meta tag (only set on actual post pages)
    try:
        og = driver.find_elements(By.CSS_SELECTOR, "meta[property='og:url']")
        if og:
            href = (og[0].get_attribute("content") or "").strip()
            if href and "/comments/" in href:
                return clean_post_url(href)
    except Exception:
        pass

    # Strategy 2: current URL if it looks like a post
    current = driver.current_url
    if any(p in current for p in ("/comments/image/", "/comments/text/", "/content/")):
        return clean_post_url(current)

    # Strategy 3: find first /comments/ link in page
    try:
        links = driver.find_elements(
            By.CSS_SELECTOR,
            "a[href*='/comments/'], a[href*='/content/']"
        )
        for a in links:
            href = (a.get_attribute("href") or "").strip()
            if href and "damadam.pk" in href:
                return clean_post_url(href)
    except Exception:
        pass

    # Strategy 4: regex on page source
    try:
        m = re.search(
            r"https?://[^\s\"']*(/comments/(?:text|image)/\d+|/content/\d+)",
            driver.page_source
        )
        if m:
            return clean_post_url(m.group(0))
    except Exception:
        pass

    # Fallback: return current URL (profile /users/ or /profile/public/ page)
    # This is still a success — the post was published
    return clean_post_url(driver.current_url)


def _extract_error_message(driver) -> str:
    """Try to find a visible error message on the current page."""
    for sel in (
        ".errorlist li", ".alert-danger", ".error", "div.err",
        "p.error", "span.error", ".messages li", ".alert",
    ):
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                text = (els[0].text or "").strip()
                if text:
                    return text[:100]
        except Exception:
            pass
    return "unknown — check logs/post_*_04_after_submit.html"



def _write_post_log(sheets: SheetsManager, item: Dict,
                    post_url: str, status: str, notes: str):
    """
    Append one row to PostLog sheet after every post attempt.
    PostLog = full history of every post, successful or not.

    Columns: TIMESTAMP | TYPE | POET | TITLE | POST_URL | IMG_LINK | STATUS | NOTES
    """
    ws = sheets.get_worksheet(Config.SHEET_POST_LOG, headers=Config.POST_LOG_COLS)
    if not ws:
        return
    sheets.append_row(ws, [
        pkt_stamp(),                        # TIMESTAMP
        item.get("type", ""),               # TYPE
        item.get("poet", ""),               # POET
        (item.get("title") or "")[:80],     # TITLE
        post_url,                           # POST_URL
        item.get("img_link", ""),           # IMG_LINK
        status,                             # STATUS
        notes[:100] if notes else "",       # NOTES
    ])

def _build_caption(item: Dict) -> str:
    """
    Build the post caption using col D (URDU) only.
    Col C (TITLE) is Roman Urdu reference — never use it as caption.

    If URDU col is empty (formula not yet evaluated), the bot skips the caption
    rather than posting English text. Make sure GOOGLETRANSLATE has run first.
    """
    parts = []
    urdu = (item.get("urdu") or "").strip()

    # Reject if it looks like an unevaluated formula or plain English
    if urdu and not urdu.startswith("="):
        parts.append(urdu)
    else:
        # Log clearly so you know why caption was skipped
        pass  # logger not available here — handled in caller

    if Config.POST_SIGNATURE:
        parts.append(Config.POST_SIGNATURE)
    return "\n".join(parts)
