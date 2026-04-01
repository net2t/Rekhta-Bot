# 🌟 DD-Post-Bot - DamaDam.pk Automation

![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-active-brightgreen.svg)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey.svg)

**A sleek, modular automation bot for DamaDam.pk**  
Specialized in Rekhta poetry scraping and automated posting with Google Sheets integration

[🚀 Quick Start](#quick-start-local) • [📖 Documentation](#documentation) • [⚙️ Configuration](#configuration) • [🔧 GitHub Actions](#github-actions)

---

## ✨ Features

| 🎨 **Rekhta Mode** | 📝 **Post Mode** |
| :---------------- | :--------------- |
| Scrape beautiful poetry image cards from rekhta.org | Create automated image/text posts from queue |
| Auto-populate PostQueue sheet with metadata | Smart cooldown and duplicate detection |
| Support for Roman Urdu and Urdu translations | Rate limit handling with automatic retries |

---

## 🚀 Quick Start (Local)

### 📋 Prerequisites

- Python 3.8 or higher
- Google Cloud account with Sheets API enabled
- DamaDam.pk account

### 🔧 Installation

```bash
# Clone the repository
git clone https://github.com/net2t/Rekhta-Bot.git
cd DD-Post-Bot

# Install dependencies
pip install -r requirements.txt
```

### 🌐 Google Sheets Setup

1. **Create Google Cloud Project**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Enable **Google Sheets API** and **Google Drive API**

2. **Create Service Account**
   - Go to IAM & Admin → Service Accounts
   - Create new service account → download JSON key as `credentials.json`

3. **Share Your Sheet**
   - Create a new Google Sheet
   - Share it with the service account email (Editor access)
   - Copy the Sheet ID from the URL

### ⚙️ Environment Configuration

```bash
# Copy the template
cp .env.sample .env

# Fill in your credentials
```

**Required `.env` variables:**

```env
DD_LOGIN_EMAIL=your_damadam_username
DD_LOGIN_PASS=your_password
DD_SHEET_ID=your_google_sheet_id
CREDENTIALS_FILE=credentials.json
```

### 🎯 Initialize & Run

```bash
# Create required sheets
python main.py setup

# Run interactive menu
python main.py
```

---

## 🎮 Usage

### Interactive Menu

Running without arguments shows a beautiful numbered menu:

```bash
python main.py
```

### CLI Commands

```bash
python main.py <mode> [options]
```

#### Available Modes

| Mode | Description | Example |
| :--- | :------------- | :------- |
| `rekhta` | Scrape poetry from Rekhta | `python main.py rekhta --max 30` |
| `post` | Create posts from queue | `python main.py post --max 5` |

#### Options

| Flag | Description |
| :--- | :------------- |
| `--max N` | Process only N items (default: unlimited) |
| `--debug` | Enable verbose debug logging |
| `--headless` | Force headless browser mode |
| `--stop-on-fail` | Stop after first failure |
| `--force-wait SECONDS` | Force wait before starting |

---

## 🔧 GitHub Actions

### 🤖 Automated Scheduling

| Mode | Schedule | Action |
| :--- | :------- | :----- |
| 🎀 **Rekhta** | Every 1 hour | Scrape new poetry |
| 📝 **Post** | Every 2 hours | Create posts from queue |

### 📝 Required Secrets

Create these secrets in your GitHub repository settings:

| Secret | Description | Required |
| :----- | :------------- | :------- |
| `DD_LOGIN_EMAIL` | DamaDam username | ✅ |
| `DD_LOGIN_PASS` | DamaDam password | ✅ |
| `DD_SHEET_ID` | Google Sheets ID | ✅ |
| `GOOGLE_CREDENTIALS_JSON` | Service account JSON | ✅ |
| `DD_LOGIN_EMAIL2` | Backup account username | ❌ |
| `DD_LOGIN_PASS2` | Backup account password | ❌ |

---

## 📊 Sheet Structure

### 📝 PostQueue

**Populated by Rekhta Mode, consumed by Post Mode**

| Column | Description |
| :----- | :------------- |
| `STATUS` | Pending → Done/Failed/Repeating |
| `TYPE` | image or text |
| `TITLE` | Roman Urdu first line |
| `URDU` | Urdu caption (use `=GOOGLETRANSLATE()`) |
| `IMG_LINK` | Full image URL from Rekhta |
| `POET` | Poet name |
| `POST_URL` | Filled by bot after posting |
| `ADDED` | Timestamp when scraped |
| `NOTES` | Error details |

### 📋 PostLog

**History of all posts created**

| Column | Description |
| :----- | :------------- |
| `TIMESTAMP` | PKT timestamp |
| `TYPE` | image or text |
| `POET` | Poet name |
| `TITLE` | Roman Urdu title |
| `POST_URL` | URL of created post |
| `IMG_LINK` | Source image URL |
| `STATUS` | Posted/Failed/Repeating/Skipped |
| `NOTES` | Error details |

---

## 🏗️ Project Structure

```
DD-Post-Bot/
├── 🚀 main.py                 # Entry point + interactive menu
├── ⚙️ config.py               # All settings and configurations
├── 📋 requirements.txt
├── 🔐 .env                    # Your credentials (gitignored)
├── 🔐 .env.sample             # Configuration template
├── 🔐 credentials.json        # Google service account key
├── 📁 core/
│   ├── 🌐 browser.py          # Chrome WebDriver management
│   ├── 🔑 login.py            # Authentication handling
│   └── 📊 sheets.py           # Google Sheets operations
├── 📁 modes/
│   ├── 🎨 rekhta.py           # Rekhta poetry scraper
│   └── 📝 post.py             # Post creation automation
├── 📁 utils/
│   ├── 📝 logger.py           # Console + file logging
│   └── 🛠️ helpers.py          # Utility functions
├── 📁 .github/
│   └── 📁 workflows/
│       └── 🤖 bot.yml          # GitHub Actions workflow
└── 📁 logs/                   # Auto-generated log files
```

---

## 🎯 Post Mode Rules

### ⏱️ Smart Cooldown

- **Minimum 135 seconds** between posts (DamaDam rate limit)
- Automatic timing to avoid restrictions

### 🔄 Duplicate Detection

- Checks if `IMG_LINK` was already posted
- Marks duplicates as `Repeating` and skips them
- Prevents spam and maintains content quality

### ⚡ Error Handling

- **Rate limit hit**: Wait required time → retry once
- **Other errors**: Mark as `Failed`, continue with next item
- **Never retry failed items automatically**

---

## 🌟 Advanced Features

### 🎨 Rekhta Mode Highlights

- **Smart Pagination**: Resumes from last scraped page
- **Rich Metadata**: Captures poet name, title, and image URLs
- **Error Recovery**: Handles network issues gracefully
- **Rate Limiting**: Respectful scraping with delays

### 📝 Post Mode Highlights

- **Image & Text Support**: Handles both media types
- **Automatic Captions**: Supports Urdu translation formulas
- **Status Tracking**: Complete audit trail in PostLog
- **Flexible Scheduling**: Manual or automated posting

---

## 🛠️ Troubleshooting

### 🔧 Common Issues

| Issue | Solution |
| :---- | :------- |
| **Browser fails to start** | Install Chrome/Chromium and update WebDriver |
| **Sheets connection error** | Check credentials.json and sheet sharing permissions |
| **Login failed** | Verify DamaDam credentials and try manual login first |
| **Rate limited** | Increase cooldown time in config or use `--force-wait` |

### 📝 Debug Mode
Enable verbose logging:
```bash
python main.py rekhta --debug
```

### 📊 View Logs
Check recent activity:
```bash
python main.py logs  # (if logs mode is available)
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit changes: `git commit -m 'Add feature'`
4. Push to branch: `git push origin feature-name`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **Rekhta.org** for the beautiful poetry collection
- **DamaDam.pk** platform
- **Google Sheets API** for data management
- **Selenium** for web automation

---

### 🌟 Star this repository if you find it helpful!

[🔝 Back to Top](#-dd-post-bot---damadampk-automation)

Made with ❤️ for automation enthusiasts
