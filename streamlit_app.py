import os
from pathlib import Path

import gspread
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _load_env() -> None:
    load_dotenv(dotenv_path=_repo_root() / ".env", override=False)


@st.cache_resource(show_spinner=False)
def _gs_client(credentials_file: str) -> gspread.Client:
    creds_path = Path(credentials_file)
    if not creds_path.is_absolute():
        creds_path = _repo_root() / creds_path

    if not creds_path.exists():
        raise FileNotFoundError(f"Credentials file not found: {creds_path}")

    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scope)
    return gspread.authorize(creds)


def _sanitize_headers(headers: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: dict[str, int] = {}

    for i, h in enumerate(headers):
        base = str(h).strip()
        if not base:
            base = f"col_{i + 1}"

        count = seen.get(base, 0) + 1
        seen[base] = count

        if count == 1:
            cleaned.append(base)
        else:
            cleaned.append(f"{base}_{count}")

    return cleaned


def _worksheet_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = _sanitize_headers(values[0])
    rows = values[1:]
    df = pd.DataFrame(rows, columns=headers)

    df.columns = [str(c).strip() for c in df.columns]
    for col in df.columns:
        df[col] = df[col].astype(str)

    return df


def _status_metrics(df: pd.DataFrame) -> dict:
    status_col = None
    for c in df.columns:
        if c.strip().lower() == "status":
            status_col = c
            break

    if not status_col:
        return {}

    s = df[status_col].fillna("").astype(str).str.strip()
    total = int(len(s))
    pending = int((s.str.lower() == "pending").sum())
    done = int(s.str.lower().isin(["done", "success", "posted"]).sum())
    failed = int(s.str.lower().isin(["failed", "error"]).sum())
    other = total - pending - done - failed

    return {
        "total": total,
        "pending": pending,
        "done": done,
        "failed": failed,
        "other": other,
        "status_col": status_col,
    }


def main() -> None:
    st.set_page_config(page_title="DD Sheet Dashboard", layout="wide")

    _load_env()

    sheet_id = os.getenv("DD_SHEET_ID", "").strip()
    credentials_file = os.getenv("CREDENTIALS_FILE", "credentials.json").strip()

    st.title("DD Sheet Dashboard")

    if not sheet_id:
        st.error("Missing DD_SHEET_ID in .env")
        st.stop()

    try:
        client = _gs_client(credentials_file)
        workbook = client.open_by_key(sheet_id)
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        st.stop()

    worksheets = workbook.worksheets()
    ws_titles = [ws.title for ws in worksheets]

    with st.sidebar:
        st.header("Controls")
        selected_title = st.selectbox("Worksheet", ws_titles, index=0 if ws_titles else None)
        refresh = st.button("Refresh")

    if refresh:
        _gs_client.clear()
        st.rerun()

    if not selected_title:
        st.info("No worksheets found in this spreadsheet.")
        st.stop()

    ws = workbook.worksheet(selected_title)
    df = _worksheet_to_df(ws)

    left, right = st.columns([2, 1])

    with left:
        st.subheader(f"Worksheet: {selected_title}")
        st.caption(f"Rows: {len(df)} | Columns: {len(df.columns)}")
        st.dataframe(df, width="stretch", height=600)

    with right:
        st.subheader("Metrics")
        if df.empty:
            st.info("Worksheet is empty.")
        else:
            metrics = _status_metrics(df)
            if metrics:
                c1, c2 = st.columns(2)
                c1.metric("Total", metrics["total"])
                c2.metric("Pending", metrics["pending"])

                c3, c4 = st.columns(2)
                c3.metric("Done", metrics["done"])
                c4.metric("Failed", metrics["failed"])

                if metrics["other"] > 0:
                    st.metric("Other", metrics["other"])

                status_series = df[metrics["status_col"]].fillna("").astype(str).str.strip()
                counts = status_series.replace({"": "(blank)"}).value_counts().head(20)
                st.bar_chart(counts)
            else:
                st.info("No STATUS column detected in this worksheet.")


if __name__ == "__main__":
    main()
