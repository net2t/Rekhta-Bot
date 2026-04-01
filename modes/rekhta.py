"""
modes/rekhta.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Rekhta Mode: Scrape poetry image cards from rekhta.org/shayari-image
and populate the PostQueue sheet.

What it does:
  1. Reads the last scraped page index from ScrapeState sheet (resumes where it left off)
  2. Opens rekhta.org CollectionLoading API pages sequentially
  3. Parses each card for: image URL, Roman Urdu text, poet name
  4. Writes Urdu caption as a Google Sheets GOOGLETRANSLATE() formula
  5. BATCH duplicate check: loads all existing IMG_LINK values from PostQueue at start
  6. Appends only NEW entries to PostQueue with STATUS=Pending
  7. Updates ScrapeState with the last successfully processed page index

Pagination cursor:
  ScrapeState sheet holds key="rekhta_last_page" → last page index scraped.
  On next run, resumes from that page + 1 (no re-scraping of already-seen pages).
  Reset by clearing ScrapeState or setting rekhta_last_page to 0.

Card HTML structure:
  div.shyriImgBox
    div.shyriImg
      a.shyriImgInner[style*='url(...)']   ← background-image contains _small or _medium URL
        img[data-src=...]                   ← direct image URL (webp, fallback jpg)
    div.shyriImgFooter
      p.shyriImgLine > a                   ← Roman Urdu first line text
      h4.shyriImgPoetName > a              ← Poet name
"""

import re
import time
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin, urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from config import Config
from utils.logger import Logger, pkt_stamp
from core.sheets import SheetsManager

# ── State key used in ScrapeState sheet ───────────────────────────────────────
_STATE_KEY_LAST_PAGE = "rekhta_last_page"
# Max pages to scan per run when unlimited (stops on consecutive blank pages)
_MAX_UNLIMITED_PAGES = 9999
# Stop if this many consecutive pages return zero new items
_MAX_NO_NEW_PAGES    = 3


def run(driver, sheets: SheetsManager, logger: Logger,
        max_items: int = 0) -> Dict:
    """
    Run Rekhta Mode end-to-end.

    Args:
        driver:    Selenium WebDriver (login not required for Rekhta)
        sheets:    Connected SheetsManager
        logger:    Logger
        max_items: 0 = add all new found; N = stop after N new rows added

    Returns:
        Stats dict: {added, skipped_dup, total_scraped, last_page}
    """
    import time as _time
    run_start = _time.time()

    logger.section("REKHTA MODE")

    # ── Get PostQueue sheet ───────────────────────────────────────────────────
    ws = sheets.get_worksheet(Config.SHEET_POST_QUE, headers=Config.POST_QUE_COLS)
    if not ws:
        logger.error("PostQueue sheet not found")
        return {}

    headers = [c for c in Config.POST_QUE_COLS]
    col_img = sheets.get_col(headers, "IMG_LINK")

    # ── BATCH duplicate check — load all existing IMG_LINK values at once ────
    existing_img_links: Set[str] = set()
    if col_img:
        raw_col = sheets.read_col_values(ws, col_img)
        for v in raw_col:
            if v:
                existing_img_links.add(_normalize_img_url(v))
        logger.info(f"Existing PostQueue entries: {len(existing_img_links)} image URLs loaded")

    # ── Read pagination cursor ────────────────────────────────────────────────
    # Resume from the last page scraped in the previous run.
    raw_cursor = sheets.get_scrape_state(_STATE_KEY_LAST_PAGE)
    try:
        start_page = max(1, int(raw_cursor) + 1) if raw_cursor else 1
    except (ValueError, TypeError):
        start_page = 1

    if start_page > 1:
        logger.info(f"Resuming from page {start_page} (cursor from ScrapeState)")
    else:
        logger.info("Starting fresh from page 1")

    # ── Calculate page budget ────────────────────────────────────────────────
    if max_items and max_items > 0:
        # Need enough pages to collect max_items new items.
        # A page typically has 9–12 cards; use 8 as conservative lower bound.
        needed_pages = start_page + int(max_items / 8) + 5
        total_pages  = max(needed_pages, start_page + 20)
    else:
        # Unlimited — scan until consecutive blank pages or end of content
        total_pages = start_page + _MAX_UNLIMITED_PAGES

    base_url    = "https://www.rekhta.org"
    added       = 0
    dup_count   = 0
    total_scraped = 0
    seen_texts: Set[str] = set()
    no_new_pages = 0
    last_good_page = start_page - 1  # tracks the last page that had content

    for page_index in range(start_page, total_pages + 1):
        page_url = _rekhta_page_url(page_index)
        logger.info(f"Loading page {page_index}: {page_url}")
        cards = []
        page_loaded = False
        for attempt in range(1, 3):
            try:
                driver.get(page_url)
            except TimeoutException:
                try:
                    driver.execute_script("window.stop();")
                except Exception:
                    pass
                logger.warning(f"Page load timeout on pageIndex={page_index} (attempt {attempt}); retrying")
                continue

            try:
                WebDriverWait(driver, 10).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.shyriImgBox")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, "body")),
                    )
                )
            except Exception:
                pass

            # Give a short settle time for lazy content
            time.sleep(0.6)
            cards = driver.find_elements(By.CSS_SELECTOR, "div.shyriImgBox")
            if cards:
                page_loaded = True
                break

            # If no cards, try one more short wait and retry once.
            time.sleep(1.5)
            cards = driver.find_elements(By.CSS_SELECTOR, "div.shyriImgBox")
            if cards:
                page_loaded = True
                break

            logger.debug(f"No cards detected on attempt {attempt} for pageIndex={page_index}")

        logger.debug(f"Cards found on pageIndex={page_index}: {len(cards)}")

        if not cards:
            logger.debug(f"No cards on pageIndex={page_index}; may be end of content or slow load")
            no_new_pages += 1
            if no_new_pages >= _MAX_NO_NEW_PAGES:
                logger.info(f"No content on {no_new_pages} consecutive pages — stopping")
                break
            continue

        page_new_added = 0
        for card in cards:
            item = _parse_card_elem(card, logger, base_url=base_url)
            if not item:
                continue

            total_scraped += 1
            norm_url  = _normalize_img_url(item["img_link"])
            norm_text = item["roman_text"].strip().lower()[:60]

            if norm_url in existing_img_links:
                dup_count += 1
                continue
            if norm_text and norm_text in seen_texts:
                dup_count += 1
                continue

            # ── Append to sheet ───────────────────────────────────────────────
            urdu_formula = (
                '=GOOGLETRANSLATE('
                'INDIRECT("C"&ROW())&" - by "&INDIRECT("F"&ROW()),'
                '"en","ur")'
            )
            title = item["roman_text"].strip()
            row_values = [
                "Pending",        # STATUS
                "image",          # TYPE
                title,            # TITLE
                urdu_formula,     # URDU
                item["img_link"], # IMG_LINK
                item["poet"],     # POET
                "",               # POST_URL
                pkt_stamp(),      # ADDED
                "",               # NOTES
            ]

            if sheets.append_row(ws, row_values):
                added += 1
                page_new_added += 1
                existing_img_links.add(norm_url)
                seen_texts.add(norm_text)
                logger.ok(f"Added: {item['poet']} — {item['roman_text'][:40]}")
            else:
                logger.error(f"Failed to append row for: {item['roman_text'][:40]}")

            time.sleep(0.4)

            if max_items and added >= max_items:
                logger.info(f"Reached max_items={max_items}; stopping")
                break

        # ── Update cursor after each page with confirmed content ─────────────
        if cards and page_loaded:
            last_good_page = page_index
            sheets.set_scrape_state(_STATE_KEY_LAST_PAGE, str(page_index))

        if max_items and added >= max_items:
            break

        # Consecutive blank-new pages (content was seen but all duplicates)
        if page_new_added == 0:
            no_new_pages += 1
            if not max_items and no_new_pages >= _MAX_NO_NEW_PAGES:
                logger.info(
                    f"No new items on {no_new_pages} consecutive pages "
                    f"(last page with new content: {last_good_page}); stopping"
                )
                break
        else:
            no_new_pages = 0

    duration = _time.time() - run_start
    stats = {
        "added":         added,
        "skipped_dup":   dup_count,
        "total_scraped": total_scraped,
        "last_page":     last_good_page,
    }

    logger.section(
        f"REKHTA MODE DONE — Added:{added}  "
        f"Duplicates skipped:{dup_count}  Total scraped:{total_scraped}  "
        f"Last page:{last_good_page}"
    )

    sheets.log_run(
        "rekhta",
        {"added": added, "skipped": dup_count, "failed": 0},
        duration_s=duration,
        notes=f"Scraped pages {start_page}→{last_good_page}. Total seen: {total_scraped}",
    )

    return stats


# ════════════════════════════════════════════════════════════════════════════════
#  CARD PARSER
# ════════════════════════════════════════════════════════════════════════════════

def _parse_card_elem(card, logger: Logger, base_url: str) -> Optional[Dict]:
    """Parse one div.shyriImgBox card element."""
    try:
        detail_url = _extract_detail_url(card, base_url=base_url)
        roman_text = _extract_roman_text(card)
        poet       = _extract_poet_name(card)

        if not roman_text:
            return None

        img_link = _extract_image_url(card, detail_url=detail_url)
        if not img_link:
            return None

        return {
            "img_link":   img_link,
            "roman_text": roman_text.strip(),
            "poet":       poet.strip() if poet else "",
            "detail_url": detail_url or "",
        }
    except Exception as e:
        logger.debug(f"Card parse error: {e}")
        return None


def _extract_image_url(card, detail_url: str = "") -> str:
    """
    Extract the best available image URL from a card.

    Priority:
      1. Build _large.png from detail URL slug (most consistent)
      2. img[data-src] — lazy-load URL
      3. img[src]
      4. background-image style on the anchor
    """
    large_from_detail = _build_large_image_url(detail_url)
    if large_from_detail:
        return large_from_detail

    try:
        img = card.find_element(By.CSS_SELECTOR, "div.shyriImg img")
        data_src = (img.get_attribute("data-src") or "").strip()
        if data_src and data_src.startswith("http"):
            return _upgrade_image_size(data_src)
    except Exception:
        pass

    try:
        img = card.find_element(By.CSS_SELECTOR, "div.shyriImg img")
        src = (img.get_attribute("src") or "").strip()
        if src and src.startswith("http"):
            return _upgrade_image_size(src)
    except Exception:
        pass

    try:
        a = card.find_element(By.CSS_SELECTOR, "a.shyriImgInner")
        style = (a.get_attribute("style") or "").strip()
        m = re.search(r"url\(['\"]?(https?://[^'\")\s]+)['\"]?\)", style)
        if m:
            return _upgrade_image_size(m.group(1))
    except Exception:
        pass

    return ""


def _extract_detail_url(card, base_url: str) -> str:
    """Extract the /shayari-image/... detail page link from a card."""
    try:
        a = card.find_element(By.CSS_SELECTOR, "a.shyriImgInner")
        href = (a.get_attribute("href") or "").strip()
        if not href:
            return ""
        return urljoin(base_url, href)
    except Exception:
        return ""


def _rekhta_page_url(page_index: int) -> str:
    """Return the URL to load for a given pageIndex of Rekhta shayari-image."""
    if page_index <= 1:
        return Config.REKHTA_URL
    return (
        "https://www.rekhta.org/CollectionLoading"
        f"?lang=1&pageType=shayariImage&contentType=&keyword=&pageIndex={page_index}"
    )


def _build_large_image_url(detail_url: str) -> str:
    """Derive the _large.png image URL from a /shayari-image/... detail URL."""
    if not detail_url:
        return ""
    try:
        p    = urlparse(detail_url)
        slug = p.path.strip("/").split("/")[-1]
        if not slug:
            return ""
        return f"https://www.rekhta.org/images/shayariimages/{slug}_large.png"
    except Exception:
        return ""


def _upgrade_image_size(url: str) -> str:
    """Replace _small with _medium in Rekhta image URLs for higher resolution."""
    if not url:
        return ""
    url = re.sub(r"_medium\.webp$", "_medium.png", url)
    url = re.sub(r"_small\.webp$",  "_medium.png", url)
    url = re.sub(r"_small\.png$",   "_medium.png", url)
    url = re.sub(r"_small\.jpg$",   "_medium.jpg", url)
    return url


def _extract_roman_text(card) -> str:
    """Extract the Roman Urdu poetry line(s) from a card."""
    try:
        line_elem = card.find_element(By.CSS_SELECTOR, "p.shyriImgLine a")
        text = (line_elem.text or "").strip()
        if text:
            return re.sub(r"\s+", " ", text).strip()
    except Exception:
        pass

    try:
        share_div = card.find_element(By.CSS_SELECTOR, "div.shareSocial")
        text = (share_div.get_attribute("data-text") or "").strip()
        if text:
            return text
    except Exception:
        pass

    try:
        img = card.find_element(By.CSS_SELECTOR, "img")
        alt = (img.get_attribute("alt") or "").strip()
        if "-" in alt:
            return alt.rsplit("-", 1)[0].strip()
        return alt
    except Exception:
        pass

    return ""


def _extract_poet_name(card) -> str:
    """Extract the poet name from a card."""
    for selector in ("h4.shyriImgPoetName a", ".ShyriImgInfoPoetName a",
                     "h4.shyriImgPoetName", ".ShyriImgInfoPoetName"):
        try:
            elem = card.find_element(By.CSS_SELECTOR, selector)
            name = (elem.text or "").strip()
            if name:
                return name
        except Exception:
            continue

    try:
        img = card.find_element(By.CSS_SELECTOR, "img")
        alt = (img.get_attribute("alt") or "").strip()
        if "-" in alt:
            return alt.rsplit("-", 1)[-1].strip()
    except Exception:
        pass

    return ""


def _normalize_img_url(url: str) -> str:
    """
    Normalize an image URL for duplicate comparison.
    Strips size suffixes so _small/_medium/_large all match.
    """
    if not url:
        return ""
    u = url.lower().strip()
    u = re.sub(r"_(small|medium|large)\.(png|jpg|jpeg|webp)$", "", u)
    u = re.sub(r"\.(png|jpg|jpeg|webp)$", "", u)
    return u
