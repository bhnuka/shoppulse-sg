import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Iterable

import clickhouse_connect
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from shapely.geometry import Point, shape
except ImportError:
    Point = None
    shape = None


def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
    )


def request_json(url: str, params: Optional[dict] = None, max_retries: int = 5):
    backoff = 1.0
    for _ in range(max_retries):
        response = requests.get(url, params=params, timeout=30)
        if response.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff *= 2
            continue
        response.raise_for_status()
        return response.json()
    response.raise_for_status()


def onemap_geocode(postal_code: str) -> Optional[Tuple[float, float]]:
    url = "https://www.onemap.gov.sg/api/common/elastic/search"
    params = {
        "searchVal": postal_code,
        "returnGeom": "Y",
        "getAddrDetails": "N",
    }
    payload = request_json(url, params=params)
    results = payload.get("results", [])
    if not results:
        return None
    result = results[0]
    try:
        return float(result["LATITUDE"]), float(result["LONGITUDE"])
    except (KeyError, ValueError, TypeError):
        return None


def load_polygons(geojson_path: Path, id_keys: List[str]) -> List[Tuple[str, object]]:
    if not shape:
        raise RuntimeError("shapely is required for polygon matching")
    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    polygons = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry")
        poly = shape(geom)
        found_id = None
        for key in id_keys:
            if key in props and props[key]:
                found_id = str(props[key])
                break
        if found_id:
            polygons.append((found_id, poly))
    return polygons


def match_polygon(point: Tuple[float, float], polygons: List[Tuple[str, object]]) -> Optional[str]:
    if not polygons:
        return None
    pt = Point(point[1], point[0])
    for poly_id, poly in polygons:
        if poly.contains(pt):
            return poly_id
    return None

def remaining_postal_codes(client) -> int:
    row = client.query(
        "SELECT countDistinct(postal_code) FROM acra_entities_raw WHERE postal_code IS NOT NULL "
        "AND postal_code NOT IN (SELECT postal_code FROM dim_postal_geo)"
    ).result_rows
    return int(row[0][0]) if row else 0



def geo_enrich(
    client,
    subzone_geojson: Path,
    planning_geojson: Path,
    limit: int,
    sleep_seconds: float,
    concurrency: int,
    batch_size: int,
):
    if not Point:
        raise RuntimeError("shapely not installed; pip install shapely")

    subzone_polys = load_polygons(subzone_geojson, ["SUBZONE_C", "subzone_id", "id"])
    planning_polys = load_polygons(planning_geojson, ["PLN_AREA_C", "planning_area_id", "id"])

    rows = client.query(
        "SELECT DISTINCT postal_code FROM acra_entities_raw WHERE postal_code IS NOT NULL"
        " AND postal_code NOT IN (SELECT postal_code FROM dim_postal_geo) LIMIT %(limit)s",
        parameters={"limit": limit},
    ).result_rows

    if not rows:
        print("No new postal codes to enrich")
        return

    def chunks(items: List[str], size: int) -> Iterable[List[str]]:
        for i in range(0, len(items), size):
            yield items[i : i + size]

    postal_codes = [row[0] for row in rows]
    inserted = 0
    for batch in chunks(postal_codes, batch_size):
        results = []
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_map = {executor.submit(onemap_geocode, code): code for code in batch}
            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    coords = future.result()
                except Exception:
                    coords = None
                if not coords:
                    continue
                subzone_id = match_polygon(coords, subzone_polys)
                planning_id = match_polygon(coords, planning_polys)
                results.append([
                    code,
                    coords[0],
                    coords[1],
                    subzone_id,
                    planning_id,
                    datetime.now(timezone.utc),
                ])
        if results:
            client.insert(
                "dim_postal_geo",
                results,
                column_names=[
                    "postal_code",
                    "latitude",
                    "longitude",
                    "subzone_id",
                    "planning_area_id",
                    "updated_at",
                ],
            )
            inserted += len(results)
            print(f"Inserted {inserted} postal geo records")
        if sleep_seconds:
            time.sleep(sleep_seconds)

    print(f"Inserted {inserted} postal geo records")


def rebuild_enriched(client):
    client.command("TRUNCATE TABLE acra_entities_enriched")
    months = client.query(
        "SELECT DISTINCT toYYYYMM(registration_incorporation_date) AS m "
        "FROM acra_entities_raw WHERE registration_incorporation_date IS NOT NULL ORDER BY m"
    ).result_rows
    if not months:
        print("No rows to rebuild")
        return

    for (month,) in months:
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
        WHERE toYYYYMM(r.registration_incorporation_date) = %(month)s
        """
        client.command(sql, parameters={"month": month})
        print(f"Inserted month {month}")

    print("acra_entities_enriched rebuilt")


def main():
    parser = argparse.ArgumentParser(description="Geo enrichment for ACRA entities")
    parser.add_argument("--subzone-geojson", default="data/raw/subzone.geojson")
    parser.add_argument("--planning-geojson", default="data/raw/planning_area.geojson")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--refresh-enriched", action="store_true")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    client = get_client()
    while True:
        remaining = remaining_postal_codes(client)
        if remaining == 0:
            print("No remaining postal codes to enrich")
            break
        print(f"Remaining postal codes: {remaining}")
        geo_enrich(client, Path(args.subzone_geojson), Path(args.planning_geojson), args.limit, args.sleep, args.concurrency, args.batch_size)
        if args.refresh_enriched:
            rebuild_enriched(client)
        if not args.loop:
            break


if __name__ == "__main__":
    main()
