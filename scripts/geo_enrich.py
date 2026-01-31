import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import clickhouse_connect
import requests

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


def geo_enrich(
    client,
    subzone_geojson: Path,
    planning_geojson: Path,
    limit: int,
    sleep_seconds: float,
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

    inserted = 0
    for (postal_code,) in rows:
        coords = onemap_geocode(postal_code)
        if not coords:
            continue
        subzone_id = match_polygon(coords, subzone_polys)
        planning_id = match_polygon(coords, planning_polys)
        client.insert(
            "dim_postal_geo",
            [
                {
                    "postal_code": postal_code,
                    "latitude": coords[0],
                    "longitude": coords[1],
                    "subzone_id": subzone_id,
                    "planning_area_id": planning_id,
                    "updated_at": datetime.utcnow().isoformat(sep=" "),
                }
            ],
        )
        inserted += 1
        if inserted % 100 == 0:
            print(f"Inserted {inserted} postal geo records")
        time.sleep(sleep_seconds)

    print(f"Inserted {inserted} postal geo records")


def rebuild_enriched(client):
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
    print("acra_entities_enriched rebuilt")


def main():
    parser = argparse.ArgumentParser(description="Geo enrichment for ACRA entities")
    parser.add_argument("--subzone-geojson", default="data/raw/subzone.geojson")
    parser.add_argument("--planning-geojson", default="data/raw/planning_area.geojson")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--refresh-enriched", action="store_true")
    args = parser.parse_args()

    client = get_client()
    geo_enrich(client, Path(args.subzone_geojson), Path(args.planning_geojson), args.limit, args.sleep)
    if args.refresh_enriched:
        rebuild_enriched(client)


if __name__ == "__main__":
    main()
