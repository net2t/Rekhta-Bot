"""
utils/helpers.py — DD-Msg-Bot V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Shared helper functions used by multiple modes.

Covers:
  - Image download (URL → temp file)
  - Caption / tag text sanitization
  - Urdu script conversion via Claude API
  - PKT timestamp generation
  - URL cleaning and validation
"""

import os
import re
import tempfile
import urllib.request
import urllib.error
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

from config import Config


# ── Pakistan Standard Time ─────────────────────────────────────────────────────
PKT = timezone(timedelta(hours=5))

def now_pkt() -> datetime:
    """Return current datetime in Pakistan Standard Time."""
    return datetime.now(tz=PKT)

def pkt_stamp() -> str:
    """Human-readable PKT timestamp: '12-Mar-26 03:45 AM'"""
    return now_pkt().strftime("%d-%b-%y %I:%M %p")


# ── Image Helpers ──────────────────────────────────────────────────────────────

def _guess_ext(url: str, content_type: str = "") -> str:
    """
    Guess the file extension from URL or Content-Type header.
    Returns '.jpg' as a safe fallback.
    """
    # Try from URL first
    url_lower = url.lower().split("?")[0]
    for ext in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
        if url_lower.endswith(ext):
            return ext
    # Try from content-type header
    ct = (content_type or "").lower()
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    if "gif" in ct:
        return ".gif"
    return ".jpg"


def download_image(url: str, logger=None) -> str:
    """
    Download an image from a URL to a temporary local file.

    Args:
        url: Direct image URL (https://...)
        logger: Optional Logger instance for progress messages

    Returns:
        Absolute path to the temp file (caller must delete after use).

    Raises:
        RuntimeError: If download fails after all retries.
    """
    last_err = None
    retries  = max(1, Config.IMAGE_DOWNLOAD_RETRIES)

    for attempt in range(1, retries + 1):
        tmp_path = ""
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            )
            with urllib.request.urlopen(req, timeout=Config.IMAGE_DOWNLOAD_TIMEOUT) as resp:
                content_type = resp.headers.get("Content-Type", "")
                ext          = _guess_ext(url, content_type)

                # Write to temp file
                tmp      = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
                tmp_path = tmp.name
                try:
                    while True:
                        chunk = resp.read(64 * 1024)  # 64 KB chunks
                        if not chunk:
                            break
                        tmp.write(chunk)
                finally:
                    tmp.close()

            # Sanity check — reject suspiciously small files (likely error pages)
            if os.path.getsize(tmp_path) < 1024:
                if logger:
                    logger.warning(f"Downloaded file too small (<1KB), skipping: {url}")
                os.unlink(tmp_path)
                raise RuntimeError("Downloaded file is too small — likely an error page")

            if logger:
                logger.debug(f"Image downloaded: {tmp_path} ({os.path.getsize(tmp_path)//1024} KB)")
            return tmp_path

        except Exception as e:
            last_err = e
            # Clean up partial file if it exists
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
            if attempt < retries:
                if logger:
                    logger.warning(f"Image download attempt {attempt} failed, retrying... ({e})")
                time.sleep(3)

    raise RuntimeError(f"Image download failed after {retries} attempts: {last_err}")


# ── Text Sanitization ──────────────────────────────────────────────────────────

def _collapse_repeats(text: str, max_run: int) -> str:
    """
    Prevent spammy repeated characters.
    E.g. with max_run=6: 'aaaaaaaaaaa' → 'aaaaaa'
    """
    if not text:
        return ""
    result = []
    count  = 1
    for i in range(1, len(text)):
        if text[i] == text[i - 1]:
            count += 1
        else:
            count = 1
        if count <= max_run:
            result.append(text[i])
    return (text[0] if text else "") + "".join(result)


def sanitize_caption(text: str) -> str:
    """
    Clean a post caption/body:
      - Strip leading/trailing whitespace
      - Collapse excessive repeated characters
      - Truncate to POST_CAPTION_MAX_LEN
    """
    if not text:
        return ""
    c = str(text).strip()
    c = _collapse_repeats(c, Config.POST_MAX_REPEAT_CHARS)
    if len(c) > Config.POST_CAPTION_MAX_LEN:
        c = c[:Config.POST_CAPTION_MAX_LEN]
    return c.strip()


def sanitize_tags(text: str) -> str:
    """
    Clean post tags string:
      - Strip, collapse repeats, truncate to POST_TAGS_MAX_LEN
    """
    if not text:
        return ""
    t = str(text).strip()
    t = _collapse_repeats(t, Config.POST_MAX_REPEAT_CHARS)
    if len(t) > Config.POST_TAGS_MAX_LEN:
        t = t[:Config.POST_TAGS_MAX_LEN]
    return t.strip()


def strip_non_bmp(text: str) -> str:
    """
    Remove characters outside the Basic Multilingual Plane (U+0000–U+FFFF).
    ChromeDriver cannot type characters above U+FFFF — this prevents send_keys crashes.
    """
    if not text:
        return ""
    return "".join(ch for ch in text if ord(ch) <= 0xFFFF)


# ── URL Helpers ────────────────────────────────────────────────────────────────

def clean_post_url(url: str) -> str:
    """
    Normalize a DamaDam post URL to its canonical form:
      /comments/text/12345  or  /comments/image/12345
    Strips fragment anchors, query strings, trailing slashes.
    """
    if not url:
        return ""

    # Convert /content/12345 → /comments/image/12345
    m = re.search(r"/content/(\d+)", url)
    if m:
        return f"{Config.BASE_URL}/comments/image/{m.group(1)}"

    # Clean text post URL
    m = re.search(r"/comments/text/(\d+)", url)
    if m:
        return f"{Config.BASE_URL}/comments/text/{m.group(1)}"

    # Clean image post URL
    m = re.search(r"/comments/image/(\d+)", url)
    if m:
        return f"{Config.BASE_URL}/comments/image/{m.group(1)}"

    # Generic cleanup: strip fragments and trailing slashes
    url = re.sub(r"/\d+/#reply$", "", url)
    url = re.sub(r"/#reply$", "", url)
    url = url.split("#")[0].split("?")[0].rstrip("/")
    return url.strip()


def is_valid_post_url(url: str) -> bool:
    """Return True if URL looks like a valid DamaDam post URL."""
    if not url:
        return False
    return (
        "damadam.pk" in url and
        any(p in url for p in ["/comments/text/", "/comments/image/", "/content/"])
    )


def is_share_or_denied_url(url: str) -> bool:
    """
    Return True if the URL indicates a post was denied or still on the share/upload page.
    Used by Post Mode to detect failed submissions.
    """
    if not url:
        return True
    return any(segment in url for segment in [
        "/share/",
        "/upload/",
        "/photo/upload/",
        "/share/text/",
        "/share/photo/",
    ])
