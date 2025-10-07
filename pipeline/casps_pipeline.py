"""CASP pipeline: fetch, normalise, diff, and export ESMA CASP register data."""
from __future__ import annotations

import dataclasses
import json
import os
import re
from typing import Any

import pandas as pd
import requests

from .esma_common import (
    COUNTRY_MAP,
    DiffResult,
    diff_against_state,
    download_csv,
    ensure_directories,
    fetch_csv_url,
    hash_row,
    normalize_columns,
    parse_date,
    write_dataset,
)

CASP_CSV_PATTERN = re.compile(r"/(CASP|CASPS)[^/]*\\.csv$", re.IGNORECASE)
CASP_URL_ENV = "CASP_CSV_URL"
CASP_TABLE = "casps"

SERVICE_CODE_MAP = {
    "a": "custody",
    "b": "trading platform",
    "c": "exchange funds",
    "d": "exchange crypto",
    "e": "execution",
    "f": "placing",
    "g": "RTO",
    "h": "advice",
    "i": "portfolio mgmt",
    "j": "transfer",
}

DATE_COLUMNS = [
    "ac_authorisation_notification_date",
    "ac_authorisation_end_date",
    "ac_lastupdate",
]


def shorten_service_codes(value: Any) -> str:
    if pd.isna(value) or value == "":
        return ""
    parts = [part.strip() for part in str(value).split("|")]
    cleaned: list[str] = []
    for part in parts:
        if not part:
            continue
        match = re.match(r"([a-j])\.", part, re.IGNORECASE)
        if match:
            code = match.group(1).lower()
            cleaned.append(SERVICE_CODE_MAP.get(code, part.strip()))
        else:
            cleaned.append(part.strip())
    # deduplicate while preserving order
    deduped = list(dict.fromkeys(cleaned))
    return " | ".join(deduped)


def normalize_casp_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    for column in list(df.columns):
        df[f"raw_{column}"] = df[column]

    df["ae_competent_authority"] = df["ae_competent_authority"].astype(str).str.strip()
    df["ae_home_member_state"] = (
        df["ae_home_member_state"].astype(str).str.strip().map(lambda x: COUNTRY_MAP.get(x.upper(), x))
    )
    df["ae_lei_name"] = df["ae_lei_name"].astype(str).str.strip()
    df["ae_website"] = df["ae_website"].astype(str).str.strip()
    if "ac_service_code" in df.columns:
        df["ac_service_code_short"] = df["ac_service_code"].map(shorten_service_codes)
    else:
        df["ac_service_code_short"] = ""

    for column in DATE_COLUMNS:
        if column in df.columns:
            df[column] = df[column].map(parse_date)

    df["pk"] = (
        df["ae_home_member_state"].fillna("").str.upper()
        + "|"
        + df["ae_competent_authority"].fillna("").str.lower()
        + "|"
        + df["ae_lei_name"].fillna("").str.lower()
    )

    business_cols = [
        "ae_competent_authority",
        "ae_home_member_state",
        "ae_lei_name",
        "ae_website",
        "ac_service_code_short",
    ]
    for column in DATE_COLUMNS:
        if column in df.columns:
            business_cols.append(column)
    df["hash"] = df.apply(lambda row: hash_row(row, business_cols), axis=1)
    return df


def build_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    export_columns = [
        "pk",
        "ae_competent_authority",
        "ae_home_member_state",
        "ae_lei_name",
        "ae_website",
        "ac_service_code_short",
    ]
    for column in DATE_COLUMNS:
        if column in df.columns:
            export_columns.append(column)
    export_df = df[export_columns].copy()
    export_df = export_df.rename(
        columns={
            "ae_competent_authority": "competent_authority",
            "ae_home_member_state": "home_member_state",
            "ae_lei_name": "lei_name",
            "ae_website": "website",
            "ac_service_code_short": "service_codes",
        }
    )
    return export_df


def run(session: requests.Session | None = None) -> dict[str, Any]:
    ensure_directories()
    session = session or requests.Session()
    session.trust_env = False

    url = os.environ.get(CASP_URL_ENV) or fetch_csv_url(CASP_CSV_PATTERN, session=session)
    df = download_csv(url, session=session)
    normalized = normalize_casp_dataframe(df)
    if normalized.empty:
        raise ValueError("CASP dataset is empty")

    diff = diff_against_state(normalized, CASP_TABLE)
    export_df = build_export_dataframe(normalized)

    write_dataset(
        name="casps",
        export_df=export_df,
        diff=diff,
        meta_extra={
            "source_url": url,
            "date_columns": [column for column in DATE_COLUMNS if column in normalized.columns],
        },
    )

    return {"url": url, "rows": int(export_df.shape[0]), "diff": dataclasses.asdict(diff)}


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2))

