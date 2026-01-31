import argparse
import os
import re
import sys
from pathlib import Path

import clickhouse_connect

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import importlib.util


def load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {module_name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module




def resolve_geojson(path_str, patterns):
    path = Path(path_str)
    if path.exists():
        return path
    search_dirs = [Path("data/map"), Path("data/raw"), path.parent]
    for directory in search_dirs:
        if not directory.exists():
            continue
        for file in directory.glob("*.geojson"):
            name = file.name.lower()
            for pattern in patterns:
                if re.search(pattern, name):
                    return file
    return path

def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
    )




def drop_tables(client):
    # Drop materialized views first, then tables
    statements = [
        "DROP VIEW IF EXISTS mv_new_entities_monthly_by_ssic",
        "DROP VIEW IF EXISTS mv_new_entities_monthly_by_subzone",
        "DROP VIEW IF EXISTS mv_new_entities_monthly_by_planning_area",
        "DROP VIEW IF EXISTS mv_top_ssic_by_area_month",
        "DROP TABLE IF EXISTS new_entities_monthly_by_ssic",
        "DROP TABLE IF EXISTS new_entities_monthly_by_subzone",
        "DROP TABLE IF EXISTS new_entities_monthly_by_planning_area",
        "DROP TABLE IF EXISTS top_ssic_by_area_month",
        "DROP TABLE IF EXISTS acra_entities_enriched",
        "DROP TABLE IF EXISTS acra_entities_raw",
        "DROP TABLE IF EXISTS dim_postal_geo",
        "DROP TABLE IF EXISTS dim_subzone",
        "DROP TABLE IF EXISTS dim_planning_area",
        "DROP TABLE IF EXISTS dim_ssic",
    ]
    for statement in statements:
        client.command(statement)
    print("Dropped existing tables/views")

def execute_schema(client, schema_path: Path):
    if not schema_path.exists():
        raise FileNotFoundError(schema_path)
    sql = schema_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for statement in statements:
        client.command(statement)
    print(f"Applied {len(statements)} schema statements")


def main():
    parser = argparse.ArgumentParser(description="Run ShopPulse SG Phase 2 pipeline")
    parser.add_argument("--schema", default="infra/clickhouse/schema.sql")
    parser.add_argument("--acra-csv", default="data/processed/acra_entities_cleaned.csv")
    parser.add_argument("--subzone-geojson", default="data/map/MasterPlan2019SubzoneBoundaryNoSeaGEOJSON.geojson")
    parser.add_argument("--planning-geojson", default="data/map/MasterPlan2025PlanningAreaBoundaryNoSea.geojson")
    parser.add_argument("--ssic-csv", default="")
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--recreate", action="store_true", help="Drop tables/views before applying schema")
    parser.add_argument("--geo-enrich", action="store_true")
    parser.add_argument("--geo-limit", type=int, default=5000)
    parser.add_argument("--geo-sleep", type=float, default=0.2)
    args = parser.parse_args()

    ingest = load_module("ingest", ROOT / "scripts" / "ingest.py")
    geo = load_module("geo_enrich", ROOT / "scripts" / "geo_enrich.py")

    client = get_client()
    client.command("SET max_partitions_per_insert_block = 2000")
    if args.recreate:
        drop_tables(client)
    execute_schema(client, Path(args.schema))

    subzone_path = resolve_geojson(args.subzone_geojson, [r"subzone"])
    planning_path = resolve_geojson(args.planning_geojson, [r"planning", r"area"])

    ingest.load_geojson(
        client,
        subzone_path,
        "dim_subzone",
        id_keys=["SUBZONE_C", "SUBZONE", "subzone_id", "id"],
        name_keys=["SUBZONE_N", "SUBZONE", "subzone", "name"],
        area_keys=["PLN_AREA_C", "planning_area_id", "planning_area"],
    )

    ingest.load_geojson(
        client,
        planning_path,
        "dim_planning_area",
        id_keys=["PLN_AREA_C", "planning_area_id", "id"],
        name_keys=["PLN_AREA_N", "planning_area", "name"],
    )

    if args.ssic_csv:
        ingest.load_ssic(client, Path(args.ssic_csv))

    ingest.load_acra_raw(client, Path(args.acra_csv), args.batch_size)
    ingest.build_enriched(client, args.truncate)

    if args.geo_enrich:
        geo.geo_enrich(
            client,
            subzone_path,
            planning_path,
            args.geo_limit,
            args.geo_sleep,
        )
        geo.rebuild_enriched(client)


if __name__ == "__main__":
    main()
