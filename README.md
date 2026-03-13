# DamaDam Bot V2.0 - Complete Documentation

## Overview

Clean, modular, multi-mode automation bot for DamaDam.pk with three complete phases:

- **Message Bot** (Phase 1): Send personal messages to targets from MsgList sheet
- **Posting Bot** (Phase 2): Create text/image posts from PostQueue sheet
- **Inbox Mails** (Phase 3): Monitor inbox, sync conversations, send replies
- **Rekhta Mode**: Populate PostQueue with Rekhta shayari-image entries

---

## 🌐 GitHub Pages Dashboard

**Live URL:** <https://net2t.github.io/DD-Msg-Bot/>

The GitHub Pages dashboard lets you:

- View all sheets (MsgList, PostQueue, Inbox, MsgHistory, Logs) in your browser
- Edit rows directly (STATUS, MESSAGE, REPLY, etc.) and **auto-sync back to Google Sheets**
- Add new targets / posts without opening Google Sheets

UI highlights:

- **Glassmorphism effects** with glowing boxed UI
- **Responsive layout** for desktop/mobile
- **Search + filters** across sheet data

### Setup for GitHub Pages

1. Go to your repo → **Settings → Pages**
2. Set **Source** to **GitHub Actions** (recommended)
3. Open `https://net2t.github.io/DD-Msg-Bot/`

Notes:

- The dashboard loads data via Google Sheets `gviz` endpoints, so the sheet must be accessible to the browser.
- If your sheet is private, you will need an authenticated proxy (not included).

---

## File Structure

```text
damadam-bot/
├── main.py                  # All bot logic (MSG, POST, INBOX, SETUP, LOGS)
├── docs/
│   └── index.html           # GitHub Pages dashboard (full SPA)
├── requirements.txt
├── .env                     # Credentials (gitignored)
├── credentials.json         # Google service account (gitignored)
└── logs/                    # Auto-created log directory
```

---

## Installation

```bash
pip install -r requirements.txt
```

### 1. Google Sheets Setup
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create project → Enable **Google Sheets API**
3. Create **Service Account** → download key as `credentials.json`
4. Share your Google Sheet with the service account email

### 2. Create `.env`

```env
DD_LOGIN_EMAIL=your_username
DD_LOGIN_PASS=your_password
DD_SHEET_ID=your_sheet_id
CREDENTIALS_FILE=credentials.json
DD_HEADLESS=1
DD_MAX_PROFILES=0
```

See `.env.sample` for all options.

---

## Usage

### Interactive (local)

```bash
python main.py
```

Interactive menu labels:

- **Message Bot**
- **Rekhta Mode**
- **Posting Bot**
- **Inbox Mails**
  - **Check Inbox**
  - **Activity History**
- **Log Reports**
- **Setup Sheets**

### CLI

```bash
python main.py --mode msg    --no-menu --max-profiles 20
python main.py --mode post   --no-menu
python main.py --mode inbox  --no-menu
python main.py --mode setup  --no-menu
python main.py --mode populate --no-menu --populate-limit 10 --populate-write
```

Notes:

- **Rekhta Mode limits**: `--populate-limit` is honored (not capped at 50). Increase `DD_REKHTA_MAX_SCROLLS` if you want the listing page to load more cards.
- **Posting Bot cooldown**: waits **123 seconds** after a successful post, and **5 seconds** after duplicate/failure.

### Inbox Mode Notes

- **Conversation dedupe**: Inbox sync keeps **1 row per nick** (case-insensitive). This avoids duplicate rows when the inbox page shows multiple blocks for the same nick.
- **Reply sending**: Put your reply in `MY_REPLY` and set `STATUS=pending` in the Inbox sheet. Bot sends via the inline form on `/inbox/`.
- **Activity feed pagination**: Activity fetch supports pagination (`/inbox/activity/?page=2`, etc.). Inbox mode fetches up to **60** latest activity items across up to **5** pages and logs them to the `Logs` sheet.
- **Clean activity logs**: Activity text is stored in `Logs.details` as multi-line text with UI noise removed (no `►` or `REMOVE`).

---

## Dashboard README (Merged)

This README also includes the dashboard documentation that previously lived in `README_DASHBOARD.md`.

---

## Sheet Structure

| Sheet | Purpose |
|-------|---------|
| MsgList | Targets for MSG mode. STATUS=pending to queue |
| MsgQueue | Message queue/history (depending on your usage) |
| PostQueue | Posts to publish. STATUS=pending, TYPE=image/text |
| PostQueueLog | Log of all published posts |
| InboxQueue | Inbox conversations. Fill MY_REPLY, bot sends on next inbox run |
| MasterLog | Activity log from all modes |

---

**Version:** 2.1.0 | **Last Updated:** February 2026
