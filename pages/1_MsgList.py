"""
Page 1 – MsgList
Send personal messages to targets.
"""

import os
import sys
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=False)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from main import Config, SheetsManager, Logger

# ── helpers ────────────────────────────────────────────────────────────────

def _make_logger():
    return Logger("streamlit_msglist")

@st.cache_resource(show_spinner=False)
def _sheets():
    log = _make_logger()
    sm = SheetsManager(log)
    if not sm.connect():
        return None, log
    return sm, log

def _load_df(sheets_mgr, sheet_id, sheet_name):
    ws = sheets_mgr.get_sheet(sheet_id, sheet_name, create_if_missing=True)
    if ws is None:
        return None, None
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame(), ws
    headers = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=headers)
    df.insert(0, "_row", range(2, 2 + len(df)))  # 1-based sheet row
    return df, ws

def _save_row(sheets_mgr, ws, df_row, original_df):
    """Write a single edited row back to the sheet."""
    row_num = int(df_row["_row"])
    headers = [c for c in original_df.columns if c != "_row"]
    for col_idx, col in enumerate(headers, start=1):
        val = str(df_row.get(col, ""))
        sheets_mgr.update_cell(ws, row_num, col_idx, val)

# ── UI ─────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="MsgList", page_icon="💬", layout="wide")
st.title("💬 MsgList — Target Messages")

sm, log = _sheets()
if sm is None:
    st.error("❌ Google Sheets connection failed. Check credentials.")
    st.stop()

sheet_id = Config.SHEET_ID
sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit" if sheet_id else ""
if sheet_url:
    st.markdown(f"📎 [Open in Google Sheets]({sheet_url})", unsafe_allow_html=False)

with st.spinner("Loading MsgList…"):
    df, ws = _load_df(sm, sheet_id, "MsgList")

if df is None:
    st.error("Could not load MsgList sheet.")
    st.stop()

if df.empty:
    st.info("MsgList is empty. Add rows below.")
else:
    # ── Filter bar ─────────────────────────────────────────────────────────
    status_col = "STATUS" if "STATUS" in df.columns else None
    filter_val = "All"
    if status_col:
        options = ["All"] + sorted(df[status_col].dropna().unique().tolist())
        filter_val = st.selectbox("Filter by STATUS", options)
        view = df if filter_val == "All" else df[df[status_col] == filter_val]
    else:
        view = df

    st.caption(f"Showing {len(view)} rows")

    # ── Editable table ─────────────────────────────────────────────────────
    display_cols = [c for c in view.columns if c != "_row"]
    edited = st.data_editor(
        view[display_cols],
        use_container_width=True,
        num_rows="dynamic",
        key="msglist_editor",
    )

    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("💾 Save Changes", type="primary"):
            with st.spinner("Saving…"):
                # Rebuild with _row column
                edited_with_row = edited.copy()
                edited_with_row.insert(0, "_row", view["_row"].values[:len(edited)])
                for _, row in edited_with_row.iterrows():
                    _save_row(sm, ws, row, df)
            st.success("✅ Saved to Google Sheets!")
            st.cache_resource.clear()
            st.rerun()
    with col2:
        if st.button("🔄 Refresh"):
            st.cache_resource.clear()
            st.rerun()

st.divider()
st.subheader("➕ Add New Target")
with st.form("add_msg"):
    cols = st.columns(3)
    mode = cols[0].selectbox("MODE", ["nick", "url"])
    name = cols[1].text_input("NAME")
    nick = cols[2].text_input("NICK/URL")
    cols2 = st.columns(3)
    city = cols2[0].text_input("CITY")
    message = cols2[1].text_area("MESSAGE", height=80)
    status = cols2[2].selectbox("STATUS", ["pending", "Done", "Failed", "Skipped"])
    submitted = st.form_submit_button("Add Row")
    if submitted:
        if not nick:
            st.warning("NICK/URL is required.")
        else:
            headers = [c for c in (df.columns if not df.empty else [])] or [
                "MODE","NAME","NICK/URL","CITY","POSTS","FOLLOWERS","Gender",
                "MESSAGE","STATUS","NOTES","RESULT URL"
            ]
            row_vals = {h: "" for h in headers}
            row_vals.update({"MODE": mode, "NAME": name, "NICK/URL": nick,
                             "CITY": city, "MESSAGE": message, "STATUS": status})
            with st.spinner("Adding row…"):
                sm.append_row(ws, [row_vals.get(h, "") for h in headers])
            st.success("✅ Row added!")
            st.cache_resource.clear()
            st.rerun()
