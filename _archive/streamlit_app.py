"""
DamaDam Bot – Streamlit Dashboard
Main home page with quick run buttons and sheet link.
"""

import os
import sys
import subprocess
import streamlit as st
from dotenv import load_dotenv

load_dotenv(override=False)
sys.path.insert(0, os.path.dirname(__file__))
from main import Config, SheetsManager, Logger, VERSION

st.set_page_config(
    page_title="DamaDam Bot Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar links ──────────────────────────────────────────────────────────
sheet_id = Config.SHEET_ID
sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit" if sheet_id else ""
github_pages_url = "https://net2t.github.io/DD-Msg-Bot/"

with st.sidebar:
    st.title("🤖 DD-Msg-Bot")
    st.caption(f"v{VERSION}")
    if sheet_url:
        st.markdown(f"[![Google Sheets](https://img.shields.io/badge/Google%20Sheets-Open-green?logo=googlesheets)]({sheet_url})")
    st.markdown(f"[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-Open-blue?logo=github)]({github_pages_url})")
    st.divider()
    st.page_link("streamlit_app.py", label="🏠 Home", icon="🏠")
    st.page_link("pages/1_MsgList.py", label="💬 MsgList", icon="💬")
    st.page_link("pages/2_PostQueue.py", label="📤 PostQueue", icon="📤")
    st.page_link("pages/3_InboxActivity.py", label="📥 Inbox & Activity", icon="📥")
    st.page_link("pages/4_MsgHistory.py", label="📜 MsgHistory", icon="📜")

# ── Main content ───────────────────────────────────────────────────────────
st.title(f"🤖 DamaDam Bot Dashboard — v{VERSION}")

if sheet_url:
    st.info(f"📎 **Google Sheet**: [{sheet_id}]({sheet_url})")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("⚡ Quick Actions")
    c1, c2, c3, c4 = st.columns(4)

    if c1.button("📨 Run MSG Mode", use_container_width=True):
        with st.spinner("Running MSG mode…"):
            result = subprocess.run(
                [sys.executable, "main.py", "--mode", "msg", "--no-menu"],
                capture_output=True, text=True, cwd=os.path.dirname(__file__)
            )
        if result.returncode == 0:
            st.success("✅ MSG mode completed!")
        else:
            st.error(f"❌ Error:\n{result.stderr[-500:]}")
        with st.expander("Output"):
            st.code(result.stdout[-2000:])

    if c2.button("📤 Run POST Mode", use_container_width=True):
        with st.spinner("Running POST mode…"):
            result = subprocess.run(
                [sys.executable, "main.py", "--mode", "post", "--no-menu"],
                capture_output=True, text=True, cwd=os.path.dirname(__file__)
            )
        if result.returncode == 0:
            st.success("✅ POST mode completed!")
        else:
            st.error(f"❌ Error:\n{result.stderr[-500:]}")
        with st.expander("Output"):
            st.code(result.stdout[-2000:])

    if c3.button("📥 Run INBOX Mode", use_container_width=True):
        with st.spinner("Running INBOX mode…"):
            result = subprocess.run(
                [sys.executable, "main.py", "--mode", "inbox", "--no-menu"],
                capture_output=True, text=True, cwd=os.path.dirname(__file__)
            )
        if result.returncode == 0:
            st.success("✅ INBOX mode completed!")
        else:
            st.error(f"❌ Error:\n{result.stderr[-500:]}")
        with st.expander("Output"):
            st.code(result.stdout[-2000:])

    if c4.button("🔧 Setup Sheets", use_container_width=True):
        with st.spinner("Running setup…"):
            result = subprocess.run(
                [sys.executable, "main.py", "--mode", "setup", "--no-menu"],
                capture_output=True, text=True, cwd=os.path.dirname(__file__)
            )
        if result.returncode == 0:
            st.success("✅ Setup completed!")
        else:
            st.error(f"❌ Error:\n{result.stderr[-500:]}")

with col2:
    st.subheader("📊 Sheet Status")
    with st.spinner("Checking sheets…"):
        try:
            log = Logger("dashboard")
            sm = SheetsManager(log)
            if sm.connect():
                sheet_names = ["MsgList", "PostQueue", "Inbox", "MsgHistory", "Logs"]
                for sn in sheet_names:
                    ws = sm.get_sheet(sheet_id, sn, create_if_missing=False)
                    if ws:
                        try:
                            row_count = len(ws.get_all_values()) - 1
                            st.success(f"✅ {sn} ({row_count} rows)")
                        except Exception:
                            st.success(f"✅ {sn}")
                    else:
                        st.warning(f"⚠️ {sn} (not found)")
            else:
                st.error("❌ Sheets connection failed")
        except Exception as e:
            st.error(f"Error: {e}")

st.divider()
st.subheader("📖 Navigation Guide")
st.markdown("""
| Page | Purpose |
|------|---------|
| 💬 **MsgList** | Add/edit targets for MSG mode. Set STATUS=pending to queue |
| 📤 **PostQueue** | Add/edit posts. Set STATUS=pending + TYPE=image/text |
| 📥 **Inbox & Activity** | View inbox conversations, add MY_REPLY, track activity logs |
| 📜 **MsgHistory** | Read-only history of all sent messages |
""")
