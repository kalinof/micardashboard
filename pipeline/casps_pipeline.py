"""CASP pipeline: fetch latest ESMA CASPs register, normalize, diff, and emit outputs."""
from __future__ import annotations

import csv
import dataclasses
import hashlib
import io
import json
import os
import re
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_PAGE_URL = "https://www.esma.europa.eu/esmas-activities/digital-finance-and-innovation/markets-crypto-assets-regulation-mica"
CASP_CSV_PATTERN = re.compile(r"/(CASP|CASPS)[^/]*\\.csv$", re.IGNORECASE)
ROOT_URL = "https://www.esma.europa.eu"
CASP_URL_ENV = "CASP_CSV_URL"

OUT_DIR = Path("out")
STATE_DB = Path("data/state.sqlite")
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

COUNTRY_MAP = {
    "AT": "Austria",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DK": "Denmark",
    "EE": "Estonia",
    "FI": "Finland",
    "FR": "France",
    "DE": "Germany",
    "GR": "Greece",
    "HU": "Hungary",
    "IE": "Ireland",
    "IT": "Italy",
    "LV": "Latvia",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MT": "Malta",
    "NL": "Netherlands",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "ES": "Spain",
    "SE": "Sweden",
    "IS": "Iceland",
    "LI": "Liechtenstein",
    "NO": "Norway",
    "UK": "United Kingdom",
    "GB": "United Kingdom",
}

DATE_COLUMNS = [
    "ac_authorisation_notification_date",
    "ac_authorisation_end_date",
    "ac_lastupdate",
]


def ensure_directories() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    STATE_DB.parent.mkdir(exist_ok=True)


def fetch_casp_url(session: requests.Session | None = None) -> str:
    session = session or requests.Session()
    resp = session.get(BASE_PAGE_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for anchor in soup.select("a[href$='.csv']"):
        href = anchor.get("href", "")
        if not href:
            continue
        if CASP_CSV_PATTERN.search(href):
            return href if href.startswith("http") else f"{ROOT_URL}{href}"
    raise RuntimeError("Could not find CASP CSV link on ESMA page")


def download_casp_dataframe(url: str, session: requests.Session | None = None) -> pd.DataFrame:
    if url.startswith("file://"):
        return pd.read_csv(url[7:])
    if not url.startswith("http"):
        return pd.read_csv(url)
    session = session or requests.Session()
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    # handle BOM and ensure UTF-8 decoding
    content = resp.content.decode("utf-8-sig")
    return pd.read_csv(io.StringIO(content))


def to_snake_case(name: str) -> str:
    name = name.strip().replace(" ", "_")
    snake = re.sub("(?<!^)(?=[A-Z])", "_", name).lower()
    snake = snake.replace("__", "_")
    return snake


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {col: to_snake_case(col) for col in df.columns}
    df = df.rename(columns=renamed)
    return df


def parse_date(value: Any) -> str | None:
    if pd.isna(value) or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        parsed = pd.to_datetime(value, errors="raise", utc=False)
    except Exception:
        return None
    if isinstance(parsed, pd.Series):
        parsed = parsed.iloc[0]
    if isinstance(parsed, pd.Timestamp):
        return parsed.date().isoformat()
    if isinstance(parsed, datetime):
        return parsed.date().isoformat()
    return None


def shorten_service_codes(value: Any) -> str:
    if pd.isna(value) or value == "":
        return ""
    parts = [part.strip() for part in str(value).split("|")]
    cleaned: List[str] = []
    for part in parts:
        if not part:
            continue
        match = re.match(r"([a-j])\.", part, re.IGNORECASE)
        if match:
            code = match.group(1).lower()
            cleaned.append(SERVICE_CODE_MAP.get(code, part.strip()))
        else:
            cleaned.append(part.strip())
    return " | ".join(dict.fromkeys(cleaned))


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
    df["ac_service_code_short"] = df["ac_service_code"].map(shorten_service_codes)

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


def hash_row(row: pd.Series, columns: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for column in columns:
        digest.update(str(row.get(column, "")).encode("utf-8"))
    return digest.hexdigest()


@dataclass
class DiffResult:
    new: List[str]
    updated: List[str]
    removed: List[str]


def diff_against_state(df: pd.DataFrame, table: str = CASP_TABLE) -> DiffResult:
    STATE_DB.parent.mkdir(exist_ok=True)
    with sqlite3.connect(STATE_DB) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (pk TEXT PRIMARY KEY, hash TEXT NOT NULL)"
        )
        existing = dict(conn.execute(f"SELECT pk, hash FROM {table}"))
        new: List[str] = []
        updated: List[str] = []
        seen = set()
        for record in df[["pk", "hash"]].to_dict("records"):
            pk = record["pk"]
            seen.add(pk)
            existing_hash = existing.get(pk)
            if existing_hash is None:
                new.append(pk)
            elif existing_hash != record["hash"]:
                updated.append(pk)
        removed = [pk for pk in existing.keys() if pk not in seen]
        conn.executemany(
            f"INSERT OR REPLACE INTO {table} (pk, hash) VALUES (?, ?)",
            df[["pk", "hash"]].itertuples(index=False, name=None),
        )
    return DiffResult(new=new, updated=updated, removed=removed)


def write_outputs(df: pd.DataFrame, diff: DiffResult) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    export_cols = [
        "pk",
        "ae_competent_authority",
        "ae_home_member_state",
        "ae_lei_name",
        "ae_website",
        "ac_service_code_short",
    ]
    for column in DATE_COLUMNS:
        if column in df.columns:
            export_cols.append(column)
    export_df = df[export_cols].copy()
    export_df = export_df.rename(
        columns={
            "ae_competent_authority": "competent_authority",
            "ae_home_member_state": "home_member_state",
            "ae_lei_name": "lei_name",
            "ae_website": "website",
            "ac_service_code_short": "service_codes",
        }
    )
    export_df.to_csv(OUT_DIR / "casps.csv", index=False)
    (OUT_DIR / "casps.json").write_text(export_df.to_json(orient="records", force_ascii=False, indent=2))

    diff_records: List[Dict[str, Any]] = []
    diff_records.extend({"pk": pk, "action": "new"} for pk in diff.new)
    diff_records.extend({"pk": pk, "action": "update"} for pk in diff.updated)
    diff_records.extend({"pk": pk, "action": "remove"} for pk in diff.removed)
    with (OUT_DIR / "casps_delta.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["pk", "action"])
        writer.writeheader()
        for record in diff_records:
            writer.writerow(record)

    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "casps",
        "total_rows": int(export_df.shape[0]),
        "new_rows": len(diff.new),
        "updated_rows": len(diff.updated),
        "removed_rows": len(diff.removed),
    }
    (OUT_DIR / "meta.json").write_text(json.dumps(metadata, indent=2))


def run() -> Dict[str, Any]:
    ensure_directories()
    session = requests.Session()
    session.trust_env = False
    url = os.environ.get(CASP_URL_ENV) or fetch_casp_url(session=session)
    df = download_casp_dataframe(url, session=session)
    normalized = normalize_casp_dataframe(df)
    diff = diff_against_state(normalized)
    write_outputs(normalized, diff)
    return {"url": url, "rows": int(normalized.shape[0]), "diff": dataclasses.asdict(diff)}


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2))
