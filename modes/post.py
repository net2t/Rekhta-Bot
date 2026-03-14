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
from typing import Optional, Dict, List, Set

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from config import Config
from utils.logger import Logger, pkt_stamp
from utils.helpers import (
    download_image, sanitize_caption, sanitize_tags,
    strip_non_bmp, clean_post_url, is_share_or_denied_url
)
from core.sheets import SheetsManager


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


def run(driver, sheets: SheetsManager, logger: Logger,
        max_posts: int = 0) -> Dict:
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

    # Resolve write-columns (1-based)
    col_status   = sheets.get_col(headers, "STATUS")
    col_post_url = sheets.get_col(headers, "POST_URL")
    col_notes    = sheets.get_col(headers, "NOTES")

    if not all([col_status, col_post_url, col_notes]):
        logger.error(f"PostQueue missing required columns. Found: {headers}")
        return {}

    def cell(row, *names):
        return SheetsManager.get_cell(row, header_map, *names)

    # ── Build duplicate index (BATCH — all at once) ───────────────────────────
    # Load ALL values in the IMG_LINK column that are already Done/Failed/Repeating
    # This avoids the old O(n²) per-row comparison.
    col_img_link = sheets.get_col(headers, "IMG_LINK")
    posted_urls: Set[str] = set()
    if col_img_link:
        for row in all_rows[1:]:
            st = cell(row, "STATUS").lower()
            if st.startswith("pending"):
                continue  # pending rows are not "done"
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

        elif status == "Rate Limited":
            # DamaDam returned a rate limit — wait the required time then retry ONCE
            wait = wait_s or Config.POST_COOLDOWN_SECONDS
            logger.warning(f"Rate limited — waiting {wait}s then retrying once...")
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
                logger.error(f"Retry also failed: {result2.get('status')}")
                sheets.update_row_cells(ws, row_num, {
                    col_status: "Failed",
                    col_notes:  f"Rate limited, retry: {result2.get('status', 'Error')}",
                })
                stats["failed"] += 1

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

    logger.section(
        f"POST MODE DONE — Posted:{stats['posted']}  "
        f"Skipped:{stats['skipped']}  Failed:{stats['failed']}"
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
            return {"status": f"Upload Error: {err_text}", "url": driver.current_url}

        return {"status": "Pending Verification", "url": post_url}

    except RuntimeError as e:
        return {"status": f"Image Download Failed: {str(e)[:60]}", "url": ""}
    except Exception as e:
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
    """
    src = page_source.lower()
    # DamaDam shows messages like "2 minute" or "130 second" wait
    if "minute" in src and ("wait" in src or "please" in src or "karo" in src):
        # Extract the number of minutes if present
        m = re.search(r"(\d+)\s*minute", src)
        if m:
            return int(m.group(1)) * 60 + 15  # add buffer
        return Config.POST_COOLDOWN_SECONDS
    if "second" in src and ("wait" in src or "please" in src):
        m = re.search(r"(\d+)\s*second", src)
        if m:
            return int(m.group(1)) + 10
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
