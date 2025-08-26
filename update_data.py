#!/usr/bin/env python3
"""Fetch MiCAR dashboard data from Google Sheets and update index.html."""

from __future__ import annotations

import json
import re
from datetime import datetime
from io import StringIO

import pandas as pd
import requests

CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRWudeV0zFLqB54658hCDUgSRFfy-"
    "ADeR2JMilO-oel74hjBr1CdIB2FWufxyR2yuQJGNaBPHNYG7vh/pub?gid=0&single=true&output=csv"
)
METADATA_URL = (
    "https://docs.google.com/spreadsheets/d/1RfeiT68rH65izevXw_Upqdn0lXz-IGI83Zn3q0SBEbE/export?format=csv&gid=353293525"
)


def fetch_dataframe(url: str) -> pd.DataFrame:
    """Return a DataFrame parsed from the CSV at ``url``."""
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def clean_records(df: pd.DataFrame) -> list[dict]:
    """Return cleaned records ready for JSON serialisation."""
    # Drop rows without an issuer name
    df = df.dropna(subset=["Issuer (HQ)"])

    numeric_cols = ["Tokens", "Euro", "USD", "CZK", "GBP"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    records = []
    for i, row in df.iterrows():
        records.append(
            {
                "id": int(row.get("#", i + 1)) if pd.notna(row.get("#")) else i + 1,
                "issuer": row.get("Issuer (HQ)", ""),
                "state": row.get("Home State", ""),
                "authority": row.get("Competent Authority", ""),
                "tokens": row.get("Authorised EMT(s)", ""),
                "count": int(row.get("Tokens", 0)),
                "euro": int(row.get("Euro", 0)),
                "usd": int(row.get("USD", 0)),
                "czk": int(row.get("CZK", 0)),
                "gbp": int(row.get("GBP", 0)),
            }
        )
    return records


def fetch_snapshot_date(url: str) -> str:
    """Return snapshot date string from the metadata CSV."""
    response = requests.get(url)
    response.raise_for_status()
    meta = pd.read_csv(StringIO(response.text), header=None, names=["key", "value"])
    meta_dict = dict(zip(meta["key"], meta["value"]))
    return meta_dict.get("snapshot_date", "")


def update_html(data: list[dict], snapshot_date: str) -> None:
    """Replace the data block and snapshot date inside index.html."""
    with open("index.html", "r", encoding="utf-8") as fh:
        content = fh.read()

    # Update snapshotDate constant
    content = re.sub(
        r"const snapshotDate = '.*?';",
        f"const snapshotDate = '{snapshot_date}';",
        content,
    )

    # Update human-readable date in the header
    try:
        human_date = datetime.strptime(snapshot_date, "%Y-%m-%d").strftime("%d %B %Y")
        content = re.sub(
            r"(Source: ESMA EMT Register, )[^<]+",
            r"\1" + human_date,
            content,
        )
    except ValueError:
        pass  # leave header unchanged if date format unexpected

    # Insert new data JSON
    data_json = json.dumps(data, indent=4, ensure_ascii=False)
    data_json = data_json.replace("\n", "\n    ")  # indent for HTML script block
    content = re.sub(
        r"const data = \[.*?\];",
        "const data = " + data_json + ";",
        content,
        flags=re.DOTALL,
    )

    with open("index.html", "w", encoding="utf-8") as fh:
        fh.write(content)


def main() -> None:
    df = fetch_dataframe(CSV_URL)
    records = clean_records(df)
    snapshot_date = fetch_snapshot_date(METADATA_URL)
    update_html(records, snapshot_date)


if __name__ == "__main__":
    main()
