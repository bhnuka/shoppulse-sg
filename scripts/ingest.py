import argparse
import csv
import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import clickhouse_connect

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


INSERT_SETTINGS = {"max_partitions_per_insert_block": 2000}

NON_NULLABLE_COLUMNS = {
    "uen",
    "entity_name",
    "entity_status_description",
    "entity_type_description",
    "business_constitution_description",
}

def normalize_na(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "na":
        return None
    return text


def normalize_postal(value: Optional[str]) -> Optional[str]:
    text = normalize_na(value)
    if text is None:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
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
    if text.isdigit() and len(text) < 5:
        return text.zfill(5)
    return text


def normalize_date(value: Optional[str]) -> Optional[date]:
    text = normalize_na(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def clean_row(row: Dict[str, str]) -> Dict[str, Optional[str]]:
    cleaned = {col: None for col in KEEP_COLUMNS}
    for col in KEEP_COLUMNS:
        value = row.get(col)
        if col == "postal_code":
            cleaned[col] = normalize_postal(value)
        elif col in ("primary_ssic_code", "secondary_ssic_code"):
            cleaned[col] = normalize_ssic(value)
        elif col in ("registration_incorporation_date", "uen_issue_date"):
            cleaned[col] = normalize_date(value)
        else:
            cleaned[col] = normalize_na(value)
        if col in NON_NULLABLE_COLUMNS and cleaned[col] is None:
            cleaned[col] = ""
    return cleaned


def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
    )


def chunked(iterable: Iterable, size: int) -> Iterable[List]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def load_geojson(client, file_path: Path, table: str, id_keys: List[str], name_keys: List[str], area_keys: Optional[List[str]] = None):
    if not file_path.exists():
        print(f"GeoJSON not found: {file_path}")
        return

    data = json.loads(file_path.read_text(encoding="utf-8"))
    features = data.get("features", [])
    rows = []

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry")
        geom_text = json.dumps(geom, ensure_ascii=False)

        def pick(keys: List[str]):
            for key in keys:
                if key in props and props[key] not in (None, ""):
                    return str(props[key])
            return None

        item_id = pick(id_keys) or pick(name_keys) or "unknown"
        name = pick(name_keys) or item_id
        area_id = pick(area_keys) if area_keys else None

        if table == "dim_subzone":
            rows.append([item_id, name, area_id, geom_text])
        else:
            rows.append([item_id, name, geom_text])

    if rows:
        if table == "dim_subzone":
            client.insert(
                table,
                rows,
                column_names=["subzone_id", "name", "planning_area_id", "geometry"],
            )
        else:
            client.insert(
                table,
                rows,
                column_names=["planning_area_id", "name", "geometry"],
            )
        print(f"Loaded {len(rows)} rows into {table}")


def load_ssic(client, file_path: Path):
    if not file_path or not file_path.exists():
        print("SSIC lookup file not provided; skipping dim_ssic")
        return

    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            rows.append([
                normalize_na(row.get("ssic_code")) or normalize_na(row.get("SSIC_CODE")),
                normalize_na(row.get("ssic_description")) or normalize_na(row.get("SSIC_DESCRIPTION")),
                normalize_na(row.get("section")) or normalize_na(row.get("SECTION")),
                normalize_na(row.get("division")) or normalize_na(row.get("DIVISION")),
            ])
        if rows:
            client.insert("dim_ssic", rows, column_names=["ssic_code", "ssic_description", "section", "division"])
            print(f"Loaded {len(rows)} rows into dim_ssic")


def load_acra_raw(client, file_path: Path, batch_size: int):
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        buffer = []
        total = 0
        for row in reader:
            cleaned = clean_row(row)
            if not cleaned.get("uen"):
                continue
            buffer.append([cleaned.get(col) for col in KEEP_COLUMNS])
            if len(buffer) >= batch_size:
                client.insert("acra_entities_raw", buffer, column_names=KEEP_COLUMNS, settings=INSERT_SETTINGS)
                total += len(buffer)
                print(f"Inserted {total} rows into acra_entities_raw")
                buffer = []
        if buffer:
            client.insert("acra_entities_raw", buffer, column_names=KEEP_COLUMNS, settings=INSERT_SETTINGS)
            total += len(buffer)
            print(f"Inserted {total} rows into acra_entities_raw")


def build_enriched(client, truncate: bool):
    if truncate:
        client.command("TRUNCATE TABLE acra_entities_enriched")

    sql = """
    INSERT INTO acra_entities_enriched
    SELECT
        r.uen,
        r.entity_name,
        r.entity_status_description,
        r.entity_type_description,
        r.business_constitution_description,
        r.company_type_description,
        r.registration_incorporation_date,
        r.uen_issue_date,
        r.primary_ssic_code,
        r.secondary_ssic_code,
        if(r.primary_ssic_code IS NULL OR r.primary_ssic_code = '', NULL,
           if(match(r.primary_ssic_code, '^[0-9]+$') AND length(r.primary_ssic_code) < 5,
              lpad(r.primary_ssic_code, 5, '0'), r.primary_ssic_code)) AS primary_ssic_norm,
        if(r.secondary_ssic_code IS NULL OR r.secondary_ssic_code = '', NULL,
           if(match(r.secondary_ssic_code, '^[0-9]+$') AND length(r.secondary_ssic_code) < 5,
              lpad(r.secondary_ssic_code, 5, '0'), r.secondary_ssic_code)) AS secondary_ssic_norm,
        r.postal_code,
        toYYYYMM(r.registration_incorporation_date) AS registration_month,
        g.latitude,
        g.longitude,
        g.subzone_id,
        g.planning_area_id
    FROM acra_entities_raw AS r
    LEFT JOIN dim_postal_geo AS g
        ON r.postal_code = g.postal_code
    """
    client.command(sql)
    print("acra_entities_enriched populated")


def main():
    parser = argparse.ArgumentParser(description="Ingest ACRA data into ClickHouse")
    parser.add_argument("--acra-csv", default="data/processed/acra_entities_cleaned.csv")
    parser.add_argument("--subzone-geojson", default="data/raw/subzone.geojson")
    parser.add_argument("--planning-geojson", default="data/raw/planning_area.geojson")
    parser.add_argument("--ssic-csv", default="")
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--truncate", action="store_true")
    args = parser.parse_args()

    client = get_client()

    load_geojson(
        client,
        Path(args.subzone_geojson),
        "dim_subzone",
        id_keys=["SUBZONE_C", "SUBZONE", "subzone_id", "id"],
        name_keys=["SUBZONE_N", "SUBZONE", "subzone", "name"],
        area_keys=["PLN_AREA_C", "planning_area_id", "planning_area"],
    )

    load_geojson(
        client,
        Path(args.planning_geojson),
        "dim_planning_area",
        id_keys=["PLN_AREA_C", "planning_area_id", "id"],
        name_keys=["PLN_AREA_N", "planning_area", "name"],
    )

    if args.ssic_csv:
        load_ssic(client, Path(args.ssic_csv))

    load_acra_raw(client, Path(args.acra_csv), args.batch_size)
    build_enriched(client, args.truncate)


if __name__ == "__main__":
    main()
