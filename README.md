# DamaDam Bot V2.0 - Complete Documentation

## Overview

Clean, modular, multi-mode automation bot for DamaDam.pk with three complete phases:

- **MSG Mode** (Phase 1): Send personal messages to targets from MsgList sheet
- **POST Mode** (Phase 2): Create text/image posts from PostQueue sheet
- **INBOX Mode** (Phase 3): Monitor inbox, sync conversations, send replies

---

## 🌐 GitHub Pages Dashboard

**Live URL:** https://net2t.github.io/DD-Msg-Bot/

The GitHub Pages dashboard lets you:
- View all sheets (MsgList, PostQueue, Inbox, MsgHistory, Logs) in your browser
- Edit rows directly (STATUS, MESSAGE, REPLY, etc.) and **auto-sync back to Google Sheets**
- Add new targets / posts without opening Google Sheets

### Setup for GitHub Pages
1. Go to your repo → **Settings → Pages**
2. Set **Source** to `Deploy from a branch`, branch = `main`, folder = `/docs`
3. Open `https://net2t.github.io/DD-Msg-Bot/`
4. Click **⚙️ Config** and enter:
   - Your **Google Sheets API Key** (create at [Google Cloud Console](https://console.cloud.google.com/apis/credentials))
   - Your **Sheet ID** (from the URL of your Google Sheet)

> ⚠️ The API key is stored only in `localStorage`. Create a **restricted key** (Sheets API, referer limited to your Pages URL).

---

## 🖥️ Streamlit Dashboard (Local)

Run a full dashboard locally with run buttons:

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Pages:
- **Home** — quick-run buttons (MSG / POST / INBOX / Setup) + sheet status
- **MsgList** — add/edit targets with inline editor → save back to Sheets
- **PostQueue** — manage post queue with filters
- **Inbox & Activity** — view inbox conversations, fill MY_REPLY, see activity + conversation logs
- **MsgHistory** — read-only history of all sent messages

---

## File Structure

```
damadam-bot/
├── main.py                  # All bot logic (MSG, POST, INBOX, SETUP, LOGS)
├── streamlit_app.py         # Local Streamlit dashboard entry point
├── pages/
│   ├── 1_MsgList.py         # Streamlit page - targets
│   ├── 2_PostQueue.py       # Streamlit page - post queue
│   ├── 3_InboxActivity.py   # Streamlit page - inbox, activity, conv log
│   └── 4_MsgHistory.py      # Streamlit page - history
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

### CLI
```bash
python main.py --mode msg    --no-menu --max-profiles 20
python main.py --mode post   --no-menu
python main.py --mode inbox  --no-menu
python main.py --mode setup  --no-menu
python main.py --mode populate --no-menu --populate-limit 10 --populate-write
```

### Streamlit Dashboard
```bash
streamlit run streamlit_app.py
```

---

## GitHub Actions

Trigger via **Actions → DamaDam Dashboard → Run workflow**.

Required secrets: `DD_LOGIN_EMAIL`, `DD_LOGIN_PASS`, `DD_SHEET_ID`, `GOOGLE_CREDENTIALS_JSON`

---

## Sheet Structure

| Sheet | Purpose |
|-------|---------|
| MsgList | Targets for MSG mode. STATUS=pending to queue |
| PostQueue | Posts to publish. STATUS=pending, TYPE=image/text |
| Inbox | Inbox conversations. Fill MY_REPLY, bot sends on next inbox run |
| MsgHistory | Log of all sent messages |
| PostHistory | Log of all published posts |
| Logs | Activity log from all modes |
| ConversationLog | Full conversation history |

---

**Version:** 2.1.0 | **Last Updated:** February 2026
