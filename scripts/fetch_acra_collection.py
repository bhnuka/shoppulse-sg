import argparse
import csv
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

KEEP_COLUMNS = [
    "uen",
    "entity_name",
    "entity_status_description",
    "entity_type_description",
    "business_constitution_description",
    "company_type_description",
    "registration_incorporation_date",
    "uen_issue_date",
    "primary_ssic_code",
    "secondary_ssic_code",
    "postal_code",
]

COLUMN_SET = set(KEEP_COLUMNS)

DEFAULT_METADATA_BASE = "https://api-production.data.gov.sg/v2/public/api/collections"
DEFAULT_DATASTORE_BASE = "https://data.gov.sg/api/action/datastore_search"
DEFAULT_LIMIT = 1000


def normalize_na(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    if text.lower() == "na":
        return None
    return text


def normalize_postal(value: Optional[str]) -> Optional[str]:
    text = normalize_na(value)
    if text is None:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits == "":
        return None
    if len(digits) < 6:
        digits = digits.zfill(6)
    if len(digits) != 6:
        return None
    return digits


def normalize_ssic(value: Optional[str]) -> Optional[str]:
    text = normalize_na(value)
    if text is None:
        return None
    text = text.strip()
    if text.isdigit():
        if len(text) < 5:
            return text.zfill(5)
        return text
    return text


def normalize_date(value: Optional[str]) -> Optional[str]:
    text = normalize_na(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.date().isoformat()
    except ValueError:
        return None


def clean_record(record: Dict[str, object]) -> Dict[str, Optional[str]]:
    cleaned = {col: None for col in KEEP_COLUMNS}
    for key, raw in record.items():
        if key not in COLUMN_SET:
            continue
        value = normalize_na(raw)
        if key == "postal_code":
            cleaned[key] = normalize_postal(value)
        elif key in ("primary_ssic_code", "secondary_ssic_code"):
            cleaned[key] = normalize_ssic(value)
        elif key in ("registration_incorporation_date", "uen_issue_date"):
            cleaned[key] = normalize_date(value)
        else:
            cleaned[key] = value
    return cleaned


def request_json(
    session: requests.Session,
    url: str,
    params: Optional[dict] = None,
    max_retries: int = 5,
    timeout_seconds: int = 60,
) -> Tuple[dict, int]:
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        response = session.get(url, params=params, timeout=timeout_seconds)
        if response.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff *= 2
            continue
        response.raise_for_status()
        return response.json(), attempt - 1
    response.raise_for_status()


def fetch_metadata(collection_id: str, base_url: str, session: requests.Session) -> List[str]:
    url = f"{base_url}/{collection_id}/metadata"
    payload, _ = request_json(session, url)
    if payload.get("code") != 0:
        raise RuntimeError(payload.get("errorMsg") or "Metadata API error")
    data = payload.get("data", {})
    metadata = data.get("collectionMetadata", {})
    return metadata.get("childDatasets", [])


def ensure_sqlite(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path.as_posix())
    columns = ",".join(f"{col} TEXT" for col in KEEP_COLUMNS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS acra_entities ({columns})")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_acra_uen ON acra_entities (uen)")
    conn.commit()
    return conn


def upsert_record(conn: sqlite3.Connection, record: Dict[str, Optional[str]]):
    uen = record.get("uen")
    if not uen:
        return
    cursor = conn.execute(
        "SELECT registration_incorporation_date FROM acra_entities WHERE uen = ?", (uen,)
    )
    row = cursor.fetchone()
    new_date = record.get("registration_incorporation_date")
    if row is None:
        placeholders = ",".join("?" for _ in KEEP_COLUMNS)
        conn.execute(
            f"INSERT INTO acra_entities ({','.join(KEEP_COLUMNS)}) VALUES ({placeholders})",
            [record.get(col) for col in KEEP_COLUMNS],
        )
        return

    existing_date = row[0]
    if existing_date and new_date:
        if new_date <= existing_date:
            return
    elif existing_date and not new_date:
        return
    elif not existing_date and not new_date:
        return

    assignments = ",".join(f"{col} = ?" for col in KEEP_COLUMNS)
    conn.execute(
        f"UPDATE acra_entities SET {assignments} WHERE uen = ?",
        [record.get(col) for col in KEEP_COLUMNS] + [uen],
    )


def export_csv(conn: sqlite3.Connection, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(KEEP_COLUMNS)
        cursor = conn.execute(f"SELECT {','.join(KEEP_COLUMNS)} FROM acra_entities")
        for row in cursor:
            writer.writerow(row)


def export_parquet(conn: sqlite3.Connection, out_path: Path):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("pyarrow not installed; falling back to CSV output", file=sys.stderr)
        export_csv(conn, out_path.with_suffix(".csv"))
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    cursor = conn.execute(f"SELECT {','.join(KEEP_COLUMNS)} FROM acra_entities")
    batch = []
    batch_size = 50000
    for row in cursor:
        batch.append(row)
        if len(batch) >= batch_size:
            table = pa.Table.from_pylist([dict(zip(KEEP_COLUMNS, item)) for item in batch])
            if writer is None:
                writer = pq.ParquetWriter(out_path.as_posix(), table.schema)
            writer.write_table(table)
            batch = []
    if batch:
        table = pa.Table.from_pylist([dict(zip(KEEP_COLUMNS, item)) for item in batch])
        if writer is None:
            writer = pq.ParquetWriter(out_path.as_posix(), table.schema)
        writer.write_table(table)
    if writer is not None:
        writer.close()


def compute_final_stats(conn: sqlite3.Connection) -> Dict[str, object]:
    stats = {}
    cursor = conn.execute("SELECT COUNT(*) FROM acra_entities")
    total_rows = cursor.fetchone()[0]
    stats["total_rows"] = total_rows

    null_counts = {col: 0 for col in KEEP_COLUMNS}
    distinct_status = set()
    distinct_type = set()

    cursor = conn.execute(f"SELECT {','.join(KEEP_COLUMNS)} FROM acra_entities")
    for row in cursor:
        row_dict = dict(zip(KEEP_COLUMNS, row))
        for col in KEEP_COLUMNS:
            if row_dict[col] in (None, ""):
                null_counts[col] += 1
        if row_dict.get("entity_status_description"):
            distinct_status.add(row_dict["entity_status_description"])
        if row_dict.get("entity_type_description"):
            distinct_type.add(row_dict["entity_type_description"])

    null_percent = {
        col: (null_counts[col] / total_rows * 100 if total_rows else 0.0)
        for col in KEEP_COLUMNS
    }

    stats["null_counts"] = null_counts
    stats["null_percent"] = null_percent
    stats["distinct_status"] = sorted(distinct_status)
    stats["distinct_type"] = sorted(distinct_type)
    return stats


def write_report(report_path: Path, resource_reports: List[Dict[str, object]], final_stats: Dict[str, object]):
    lines = ["# ACRA Phase 1 Report", "", "## Resource IDs / Files", ""]
    for report in resource_reports:
        lines.append(f"- {report['resource_id']}")

    lines.append("")
    lines.append("## Per-resource fetch summary")
    lines.append("")
    for report in resource_reports:
        lines.append(f"### {report['resource_id']}")
        lines.append(f"- total_rows: {report['total_rows']}")
        lines.append(f"- fetched_rows: {report['fetched_rows']}")
        lines.append(f"- first_offset: {report['first_offset']}")
        lines.append(f"- last_offset: {report['last_offset']}")
        lines.append(f"- limit_used: {report['limit_used']}")
        lines.append(f"- retries: {report['retries']}")
        if report.get("errors"):
            lines.append("- errors:")
            for error in report["errors"]:
                lines.append(f"  - {error}")
        lines.append("")

    lines.append("## Final consolidated stats")
    lines.append("")
    lines.append(f"- total_rows: {final_stats['total_rows']}")
    lines.append("")
    lines.append("### Null percentages")
    lines.append("")
    for col, pct in final_stats["null_percent"].items():
        lines.append(f"- {col}: {pct:.2f}%")

    lines.append("")
    lines.append("### Distinct counts")
    lines.append("")
    lines.append(
        f"- entity_status_description: {len(final_stats['distinct_status'])}"
    )
    lines.append(
        f"- entity_type_description: {len(final_stats['distinct_type'])}"
    )

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def fetch_dataset(
    session: requests.Session,
    datastore_base: str,
    resource_id: str,
    initial_limit: int,
    fields: List[str],
    conn: sqlite3.Connection,
    timeout_seconds: int,
) -> Dict[str, object]:
    total_rows = None
    fetched_rows = 0
    first_offset = None
    last_offset = None
    retries = 0
    errors = []
    limit = initial_limit
    min_limit = 500
    offset = 0

    while True:
        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
            "fields": ",".join(fields),
        }
        try:
            payload, retry_count = request_json(
                session,
                datastore_base,
                params,
                timeout_seconds=timeout_seconds,
            )
            retries += retry_count
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None
            if status_code == 413 and limit > min_limit:
                limit = max(min_limit, limit // 2)
                offset = 0
                fetched_rows = 0
                first_offset = None
                last_offset = None
                continue
            errors.append(str(exc))
            break
        except Exception as exc:
            errors.append(str(exc))
            break

        if not payload.get("success"):
            errors.append(str(payload.get("error")) or "Unknown error")
            break

        result = payload.get("result", {})
        if total_rows is None:
            total_rows = result.get("total")
        records = result.get("records", [])
        if not records:
            break

        if first_offset is None:
            first_offset = offset
        last_offset = offset

        for record in records:
            cleaned = clean_record(record)
            upsert_record(conn, cleaned)
            fetched_rows += 1

        if total_rows is not None:
            print(f"{resource_id}: {fetched_rows}/{total_rows} rows fetched")
        else:
            print(f"{resource_id}: {fetched_rows} rows fetched")

        offset += limit
        if total_rows is not None and offset >= total_rows:
            break

    return {
        "resource_id": resource_id,
        "total_rows": total_rows,
        "fetched_rows": fetched_rows,
        "first_offset": first_offset,
        "last_offset": last_offset,
        "limit_used": limit,
        "retries": retries,
        "errors": errors,
    }


def ingest_local_csvs(
    input_dir: Path,
    conn: sqlite3.Connection,
    progress_every: int,
) -> List[Dict[str, object]]:
    reports = []
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        raise RuntimeError(f"No CSV files found in {input_dir}")

    for csv_path in csv_files:
        fetched_rows = 0
        first_offset = 0
        last_offset = 0
        errors = []
        retries = 0
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    cleaned = clean_record(row)
                    upsert_record(conn, cleaned)
                    fetched_rows += 1
                    if fetched_rows % progress_every == 0:
                        print(f"{csv_path.name}: {fetched_rows} rows ingested")
        except Exception as exc:
            errors.append(str(exc))

        last_offset = fetched_rows
        reports.append(
            {
                "resource_id": csv_path.name,
                "total_rows": None,
                "fetched_rows": fetched_rows,
                "first_offset": first_offset,
                "last_offset": last_offset,
                "limit_used": None,
                "retries": retries,
                "errors": errors,
            }
        )
        conn.commit()

    return reports


def main():
    parser = argparse.ArgumentParser(description="Fetch ACRA datasets from data.gov.sg")
    parser.add_argument("--collection-id", default="2")
    parser.add_argument(
        "--out",
        default="data/processed/acra_entities_cleaned.csv",
        help="Output file path (.parquet preferred, .csv supported)",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--report", default="docs/acra_phase1_report.md")
    parser.add_argument("--metadata-base", default=os.getenv("DG_METADATA_BASE", DEFAULT_METADATA_BASE))
    parser.add_argument("--datastore-base", default=os.getenv("DG_DATASTORE_BASE", DEFAULT_DATASTORE_BASE))
    parser.add_argument("--timeout", type=int, default=60, help="Request timeout in seconds")
    parser.add_argument("--input-dir", default="data/raw", help="Local CSV directory")
    parser.add_argument("--use-local", action="store_true", help="Use local CSV files instead of API")
    parser.add_argument("--progress-every", type=int, default=100000)
    args = parser.parse_args()

    out_path = Path(args.out)
    report_path = Path(args.report)
    db_path = out_path.parent / f"acra_entities_tmp_{int(time.time())}.db"

    session = requests.Session()
    conn = ensure_sqlite(db_path)
    resource_reports = []

    if args.use_local:
        resource_reports = ingest_local_csvs(Path(args.input_dir), conn, args.progress_every)
    else:
        resource_ids = fetch_metadata(args.collection_id, args.metadata_base, session)
        if not resource_ids:
            raise RuntimeError("No resource IDs discovered")
        for resource_id in resource_ids:
            report = fetch_dataset(
                session,
                args.datastore_base,
                resource_id,
                args.limit,
                KEEP_COLUMNS,
                conn,
                args.timeout,
            )
            conn.commit()
            resource_reports.append(report)

    if out_path.suffix.lower() == ".parquet":
        export_parquet(conn, out_path)
    else:
        export_csv(conn, out_path)

    final_stats = compute_final_stats(conn)
    write_report(report_path, resource_reports, final_stats)

    conn.close()
    try:
        db_path.unlink(missing_ok=True)
    except PermissionError:
        print(f"Warning: could not delete temp db {db_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
