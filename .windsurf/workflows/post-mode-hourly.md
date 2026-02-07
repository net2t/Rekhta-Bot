---
description: Run Post Mode hourly (Windows Task Scheduler)
---

# Overview
This workflow explains how to run **Post Mode** on an hourly schedule using **Windows Task Scheduler**.

# One-time setup
1. Make sure your `.env` is configured (sheet id, credentials file, login, etc.).
2. Verify Python and dependencies are installed:
   - `python --version`
   - `pip install -r requirements.txt`

# Optional: create / format all sheets
Run Setup mode once (creates missing sheets + applies formatting):

```
python main.py --mode setup --no-menu
```

# Manual run (smoke test)
Run Post Mode once manually:

```
python main.py --mode post --no-menu
```

If you want a safe run without posting:

```
set DD_DRY_RUN=1
python main.py --mode post --no-menu
```

# Hourly scheduling (Windows Task Scheduler)
1. Open **Task Scheduler**
2. Click **Create Task...**
3. **General** tab:
   - Name: `DD-Msg-Bot Post Mode`
   - Run whether user is logged on or not
   - Configure for: your Windows version
4. **Triggers** tab:
   - New Trigger...
   - Begin the task: On a schedule
   - Settings: Daily
   - Repeat task every: 1 hour
   - For a duration of: Indefinitely
5. **Actions** tab:
   - New Action...
   - Action: Start a program
   - Program/script: `python`
   - Add arguments:
     - `main.py --mode post --no-menu`
   - Start in:
     - `c:\Users\NADEEM\3D Objects\DD-Msg-Bot`
6. **Conditions** / **Settings**:
   - Enable: Stop the task if it runs longer than (optional)
   - Enable: If the task fails, restart every (optional)

# Notes
- Post Mode will only apply the **130 second cooldown** after a successful post.
- Any `Denied` / `Repeating` / other errors are marked as `Failed` / `Repeating` and are not retried.
