"""
Page 2 – PostQueue
Manage posts (text / image) ready to be published.
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
    log = Logger("streamlit_postqueue")
    sm = SheetsManager(log)
    if not sm.connect():
        return None, log
    return sm, log


def _load_df(sm, sheet_id, name):
    ws = sm.get_sheet(sheet_id, name, create_if_missing=True)
    if ws is None:
        return None, None
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame(), ws
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df.insert(0, "_row", range(2, 2 + len(df)))
    return df, ws


def _save_row(sm, ws, df_row, original_df):
    row_num = int(df_row["_row"])
    headers = [c for c in original_df.columns if c != "_row"]
    for i, col in enumerate(headers, 1):
        sm.update_cell(ws, row_num, i, str(df_row.get(col, "")))


# ── UI ─────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="PostQueue", page_icon="📤", layout="wide")
st.title("📤 PostQueue — Post Schedule")

sm, log = _sheets()
if sm is None:
    st.error("❌ Sheets connection failed.")
    st.stop()

sheet_id = Config.SHEET_ID
if sheet_id:
    st.markdown(f"📎 [Open in Google Sheets](https://docs.google.com/spreadsheets/d/{sheet_id}/edit)")

with st.spinner("Loading PostQueue…"):
    df, ws = _load_df(sm, sheet_id, "PostQueue")

if df is None:
    st.error("Could not load PostQueue.")
    st.stop()

if df.empty:
    st.info("PostQueue is empty.")
else:
    status_col = "STATUS" if "STATUS" in df.columns else None
    f = "All"
    if status_col:
        opts = ["All"] + sorted(df[status_col].dropna().unique().tolist())
        f = st.selectbox("Filter STATUS", opts)
        view = df if f == "All" else df[df[status_col] == f]
    else:
        view = df

    type_col = "TYPE" if "TYPE" in df.columns else None
    if type_col:
        types = ["All"] + sorted(df[type_col].dropna().unique().tolist())
        ft = st.selectbox("Filter TYPE", types)
        if ft != "All":
            view = view[view[type_col] == ft]

    st.caption(f"{len(view)} rows")
    display_cols = [c for c in view.columns if c != "_row"]
    edited = st.data_editor(view[display_cols], use_container_width=True,
                             num_rows="dynamic", key="postqueue_editor")

    c1, c2 = st.columns([1, 5])
    with c1:
        if st.button("💾 Save Changes", type="primary"):
            with st.spinner("Saving…"):
                ew = edited.copy()
                ew.insert(0, "_row", view["_row"].values[:len(edited)])
                for _, row in ew.iterrows():
                    _save_row(sm, ws, row, df)
            st.success("✅ Saved!")
            st.cache_resource.clear()
            st.rerun()
    with c2:
        if st.button("🔄 Refresh"):
            st.cache_resource.clear()
            st.rerun()

st.divider()
st.subheader("➕ Add Post")
with st.form("add_post"):
    c = st.columns(3)
    ptype = c[0].selectbox("TYPE", ["image", "text"])
    status = c[1].selectbox("STATUS", ["pending", "Done", "Failed", "Skipped", "Repeating"])
    title = c[2].text_input("TITLE")
    c2 = st.columns(2)
    image_path = c2[0].text_input("IMAGE_PATH / URL")
    title_ur = c2[1].text_input("TITLE_UR / Caption")
    tags = st.text_input("TAGS (comma separated)")
    sub = st.form_submit_button("Add")
    if sub:
        rows = ws.get_all_values() if ws else []
        headers = rows[0] if rows else [
            "STATUS","TITLE","TITLE_UR","IMAGE_PATH","TYPE",
            "POST_URL","TIMESTAMP","NOTES","SIGNATURE"
        ]
        rv = {h: "" for h in headers}
        rv.update({"STATUS": status, "TITLE": title, "TITLE_UR": title_ur,
                   "IMAGE_PATH": image_path, "TYPE": ptype, "TAGS": tags})
        with st.spinner("Adding…"):
            sm.append_row(ws, [rv.get(h, "") for h in headers])
        st.success("✅ Added!")
        st.cache_resource.clear()
        st.rerun()
