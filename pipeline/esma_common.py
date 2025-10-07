"""Shared utilities for ESMA interim register pipelines."""
from __future__ import annotations

import io
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Pattern

import pandas as pd
import requests
from bs4 import BeautifulSoup
import hashlib

BASE_PAGE_URL = (
    "https://www.esma.europa.eu/esmas-activities/digital-finance-and-innovation/"
    "markets-crypto-assets-regulation-mica"
)
ROOT_URL = "https://www.esma.europa.eu"

OUT_DIR = Path("out")
STATE_DB = Path("data/state.sqlite")
BACKUP_DIR = Path("data/backups")
META_FILE = OUT_DIR / "meta.json"


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


@dataclass
class DiffResult:
    """Represents row-level changes between snapshots."""

    new: list[str]
    updated: list[str]
    removed: list[str]

    @property
    def has_changes(self) -> bool:
        return bool(self.new or self.updated or self.removed)


def ensure_directories() -> None:
    """Ensure output and state directories exist."""

    OUT_DIR.mkdir(exist_ok=True)
    STATE_DB.parent.mkdir(exist_ok=True)
    BACKUP_DIR.mkdir(exist_ok=True, parents=True)


def fetch_csv_url(
    pattern: Pattern[str],
    *,
    session: requests.Session | None = None,
    base_url: str = BASE_PAGE_URL,
) -> str:
    """Discover the latest CSV URL for a given ESMA register pattern."""

    session = session or requests.Session()
    response = session.get(base_url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    for anchor in soup.select("a[href$='.csv']"):
        href = anchor.get("href") or ""
        if pattern.search(href):
            return href if href.startswith("http") else f"{ROOT_URL}{href}"
    raise RuntimeError("CSV link matching pattern not found on ESMA page")


def download_csv(url: str, *, session: requests.Session | None = None) -> pd.DataFrame:
    """Download a CSV into a pandas DataFrame, handling local paths and BOMs."""

    if url.startswith("file://"):
        return pd.read_csv(url[7:])
    if not url.startswith("http"):
        return pd.read_csv(url)

    session = session or requests.Session()
    response = session.get(url, timeout=60)
    response.raise_for_status()
    content = response.content.decode("utf-8-sig")
    return pd.read_csv(io.StringIO(content))


def to_snake_case(name: str) -> str:
    name = name.strip().replace(" ", "_")
    snake = pd.Series([name]).str.replace("(?<!^)(?=[A-Z])", "_", regex=True)[0]
    snake = snake.replace("__", "_")
    return snake.lower()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {column: to_snake_case(column) for column in df.columns}
    return df.rename(columns=renamed)


def parse_date(value: Any) -> str | None:
    if pd.isna(value) or value == "":
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if parsed is None or (isinstance(parsed, float) and pd.isna(parsed)):
        return None
    if isinstance(parsed, pd.Series):
        parsed = parsed.iloc[0]
    if isinstance(parsed, pd.Timestamp):
        return parsed.date().isoformat()
    if isinstance(parsed, datetime):
        return parsed.date().isoformat()
    return None


def hash_row(row: pd.Series, columns: Iterable[str]) -> str:
    sha = hashlib.sha256()
    for column in columns:
        sha.update(str(row.get(column, "")).encode("utf-8"))
    return sha.hexdigest()


def diff_against_state(df: pd.DataFrame, table: str) -> DiffResult:
    STATE_DB.parent.mkdir(exist_ok=True)
    import sqlite3

    with sqlite3.connect(STATE_DB) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (pk TEXT PRIMARY KEY, hash TEXT NOT NULL)"
        )
        existing = dict(conn.execute(f"SELECT pk, hash FROM {table}"))

        new: list[str] = []
        updated: list[str] = []
        seen: set[str] = set()
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
        if removed:
            conn.executemany(
                f"DELETE FROM {table} WHERE pk = ?",
                ((pk,) for pk in removed),
            )

    return DiffResult(new=new, updated=updated, removed=removed)


def backup_existing_outputs(dataset: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%SZ")
    dest_dir = BACKUP_DIR / dataset / timestamp
    files = {
        f"{dataset}.csv": OUT_DIR / f"{dataset}.csv",
        f"{dataset}.json": OUT_DIR / f"{dataset}.json",
        f"{dataset}_delta.csv": OUT_DIR / f"{dataset}_delta.csv",
    }
    copied_any = False
    for name, src in files.items():
        if src.exists():
            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest_dir / name)
            copied_any = True
    if not copied_any:
        dest_dir.rmdir() if dest_dir.exists() and not any(dest_dir.iterdir()) else None


def atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(data, encoding="utf-8")
    tmp_path.replace(path)


def update_meta(source: str, metadata: dict[str, Any]) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    metadata = {**metadata, "source": source}
    metadata.setdefault("generated_at", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))

    existing: dict[str, Any] = {}
    if META_FILE.exists():
        try:
            raw = json.loads(META_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            raw = {}
        if isinstance(raw, dict):
            # Support legacy meta format where dataset metadata lived at the top level
            if "source" in raw and any(key in raw for key in ("total_rows", "new_rows", "updated_rows", "removed_rows")):
                dataset_name = str(raw.get("source") or source)
                existing[dataset_name] = {
                    key: value
                    for key, value in raw.items()
                    if key not in {dataset_name, "source"}
                }
                existing[dataset_name]["source"] = dataset_name
            for key, value in raw.items():
                if isinstance(value, dict) and "source" in value:
                    existing[key] = value

    existing[source] = metadata
    atomic_write(META_FILE, json.dumps(existing, indent=2))


def write_dataset(
    *,
    name: str,
    export_df: pd.DataFrame,
    diff: DiffResult,
    meta_extra: dict[str, Any] | None = None,
) -> None:
    if export_df.empty:
        raise ValueError(f"{name} dataset is empty; refusing to overwrite outputs")

    backup_existing_outputs(name)

    csv_buffer = io.StringIO()
    export_df.to_csv(csv_buffer, index=False)
    atomic_write(OUT_DIR / f"{name}.csv", csv_buffer.getvalue())

    json_text = export_df.to_json(orient="records", force_ascii=False, indent=2)
    atomic_write(OUT_DIR / f"{name}.json", json_text)

    delta_records: list[dict[str, str]] = []
    delta_records.extend({"pk": pk, "action": "new"} for pk in diff.new)
    delta_records.extend({"pk": pk, "action": "update"} for pk in diff.updated)
    delta_records.extend({"pk": pk, "action": "remove"} for pk in diff.removed)
    delta_buffer = io.StringIO()
    pd.DataFrame(delta_records, columns=["pk", "action"]).to_csv(delta_buffer, index=False)
    atomic_write(OUT_DIR / f"{name}_delta.csv", delta_buffer.getvalue())

    meta = {
        "total_rows": int(export_df.shape[0]),
        "new_rows": len(diff.new),
        "updated_rows": len(diff.updated),
        "removed_rows": len(diff.removed),
    }
    if meta_extra:
        meta.update(meta_extra)
    update_meta(name, meta)

