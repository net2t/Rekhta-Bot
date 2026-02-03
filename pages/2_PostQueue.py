import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_utils import (
    FilterState,
    apply_filters,
    dataframe_download_button,
    find_col,
    normalize_table,
    parse_timestamp_series,
    require_config,
    status_metrics,
    style_failed_rows,
    worksheet_to_df,
)


WS_TITLE = "PostQueue"


def main() -> None:
    st.set_page_config(page_title="PostQueue", layout="wide")
    st.title("PostQueue")

    try:
        sheet_id, credentials_file = require_config()
    except Exception as e:
        st.error(str(e))
        st.stop()

    df_raw = worksheet_to_df(sheet_id, WS_TITLE, credentials_file)
    df_raw = normalize_table(df_raw)

    status_col = find_col(df_raw, ["status"])
    ts_col, ts = parse_timestamp_series(df_raw)

    with st.sidebar:
        st.header("Filters")
        statuses = []
        if status_col and not df_raw.empty:
            statuses = sorted(df_raw[status_col].fillna("").astype(str).str.strip().unique().tolist())
        status_selected = st.multiselect("Status", statuses, default=[])
        search_text = st.text_input("Search")

        date_range = None
        if ts is not None and ts.notna().sum() > 0:
            min_d = ts.min()
            max_d = ts.max()
            dr = st.date_input("Date range", value=(min_d.date(), max_d.date()))
            if isinstance(dr, tuple) and len(dr) == 2:
                start = pd.to_datetime(dr[0])
                end = pd.to_datetime(dr[1])
                date_range = (start, end)

    fs = FilterState(status_values=status_selected or None, search_text=search_text, date_range=date_range)
    df = apply_filters(df_raw, status_col, ts, fs)

    m = status_metrics(df)
    if m:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total", m["total"])
        c2.metric("Pending", m["pending"])
        c3.metric("Done", m["done"])
        c4.metric("Failed", m["failed"])

    charts = st.columns(2)

    with charts[0]:
        st.subheader("Status breakdown")
        if status_col and not df.empty:
            counts = df[status_col].fillna("").astype(str).str.strip().replace({"": "(blank)"}).value_counts().head(20)
            st.bar_chart(counts)
        else:
            st.info("No STATUS column available.")

    with charts[1]:
        st.subheader("Trend")
        if ts is not None and ts.notna().sum() > 0:
            daily = ts.dt.date.value_counts().sort_index()
            st.line_chart(daily)
        else:
            st.info("No usable TIMESTAMP column available.")

    st.subheader("Data")
    if df.empty:
        st.info("No rows match your filters.")
        return

    styler = style_failed_rows(df)
    if styler is not None:
        st.dataframe(styler, width="stretch", height=650)
    else:
        st.dataframe(df, width="stretch", height=650)

    dataframe_download_button(df, filename=f"{WS_TITLE}.csv")


if __name__ == "__main__":
    main()
