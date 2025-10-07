"""Non-compliant pipeline: fetch, normalise, diff, and export ESMA non-compliant entities."""
from __future__ import annotations

import dataclasses
import json
import os
import re

import pandas as pd
import requests

from .esma_common import (
    COUNTRY_MAP,
    diff_against_state,
    download_csv,
    ensure_directories,
    fetch_csv_url,
    hash_row,
    normalize_columns,
    write_dataset,
)

NON_COMPLIANT_CSV_PATTERN = re.compile(r"/[^/]*Non[^/]*compliant[^/]*\\.csv$", re.IGNORECASE)
NON_COMPLIANT_URL_ENV = "NON_COMPLIANT_CSV_URL"
NON_COMPLIANT_TABLE = "non_compliant"


def pick_column(df: pd.DataFrame, *candidates: str, default: str = "") -> pd.Series:
    for name in candidates:
        if name in df.columns:
            return df[name]
    return pd.Series([default] * len(df))


def normalise_country(value: str) -> str:
    upper = value.upper()
    if upper in {"", "NAN", "NONE", "NULL"}:
        return ""
    return COUNTRY_MAP.get(upper, value)


def normalize_non_compliant_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    for column in list(df.columns):
        df[f"raw_{column}"] = df[column]

    df["ae_competent_authority"] = pick_column(
        df, "ae_competent_authority", "ae_competentauthority", "competent_authority"
    ).astype(str).str.strip()

    df["ae_home_member_state"] = pick_column(
        df, "ae_home_member_state", "ae_homememberstate", "member_state"
    ).astype(str).str.strip().map(normalise_country)

    df["ae_lei_name"] = pick_column(df, "ae_lei_name", "commercial_name").astype(str).str.strip()
    df["ae_website"] = pick_column(df, "ae_website", "website").astype(str).str.strip()

    df["is_new_flag"] = False
    if "column_1" in df.columns:
        df["is_new_flag"] = (
            df["column_1"].astype(str).str.strip().str.lower() == "new"
        )

    df = df[df["ae_lei_name"].astype(bool)]

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
        "is_new_flag",
    ]
    df["hash"] = df.apply(lambda row: hash_row(row, business_cols), axis=1)
    return df


def build_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    export_columns = [
        "pk",
        "ae_competent_authority",
        "ae_home_member_state",
        "ae_lei_name",
        "ae_website",
        "is_new_flag",
    ]
    export_df = df[export_columns].copy()
    export_df = export_df.rename(
        columns={
            "ae_competent_authority": "competent_authority",
            "ae_home_member_state": "home_member_state",
            "ae_lei_name": "lei_name",
            "ae_website": "website",
            "is_new_flag": "is_new",
        }
    )
    return export_df


def run(session: requests.Session | None = None) -> dict[str, object]:
    ensure_directories()
    session = session or requests.Session()
    session.trust_env = False

    url = os.environ.get(NON_COMPLIANT_URL_ENV) or fetch_csv_url(
        NON_COMPLIANT_CSV_PATTERN, session=session
    )
    df = download_csv(url, session=session)
    normalized = normalize_non_compliant_dataframe(df)
    if normalized.empty:
        raise ValueError("Non-compliant dataset is empty")

    diff = diff_against_state(normalized, NON_COMPLIANT_TABLE)
    export_df = build_export_dataframe(normalized)

    write_dataset(
        name="non_compliant",
        export_df=export_df,
        diff=diff,
        meta_extra={"source_url": url},
    )

    return {"url": url, "rows": int(export_df.shape[0]), "diff": dataclasses.asdict(diff)}


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2))

