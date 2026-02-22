"""
Page 4 – MsgHistory
Read-only view of all sent messages.
"""

import os
import sys
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=False)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from main import Config, SheetsManager, Logger


@st.cache_resource(show_spinner=False)
def _sheets():
    log = Logger("streamlit_history")
    sm = SheetsManager(log)
    if not sm.connect():
        return None, log
    return sm, log


st.set_page_config(page_title="MsgHistory", page_icon="📜", layout="wide")
st.title("📜 Message History")

sm, log = _sheets()
if sm is None:
    st.error("❌ Sheets connection failed.")
    st.stop()

sheet_id = Config.SHEET_ID
if sheet_id:
    st.markdown(f"📎 [Open in Google Sheets](https://docs.google.com/spreadsheets/d/{sheet_id}/edit)")

with st.spinner("Loading MsgHistory…"):
    ws = sm.get_sheet(sheet_id, "MsgHistory", create_if_missing=True)
    rows = ws.get_all_values() if ws else []

if not rows or len(rows) <= 1:
    st.info("No message history yet.")
    st.stop()

df = pd.DataFrame(rows[1:], columns=rows[0])

# ── Metrics ────────────────────────────────────────────────────────────────
m1, m2, m3 = st.columns(3)
m1.metric("Total Sent", len(df))
if "STATUS" in df.columns:
    m2.metric("Success", int((df["STATUS"].str.lower().isin(["posted","done","sent"])).sum()))
    m3.metric("Failed", int((df["STATUS"].str.lower() == "failed").sum()))

# ── Filters ────────────────────────────────────────────────────────────────
c1, c2 = st.columns(2)
nick_opts = ["All"] + sorted(df["NICK"].dropna().unique().tolist()) if "NICK" in df.columns else ["All"]
nick_f = c1.selectbox("Filter NICK", nick_opts)
status_opts = ["All"] + sorted(df["STATUS"].dropna().unique().tolist()) if "STATUS" in df.columns else ["All"]
status_f = c2.selectbox("Filter STATUS", status_opts)

view = df.copy()
if nick_f != "All" and "NICK" in view.columns:
    view = view[view["NICK"] == nick_f]
if status_f != "All" and "STATUS" in view.columns:
    view = view[view["STATUS"] == status_f]

st.caption(f"{len(view)} records")
st.dataframe(view, use_container_width=True)

if st.button("🔄 Refresh"):
    st.cache_resource.clear()
    st.rerun()
