from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

from app.core.db import get_client

router = APIRouter(prefix="/api")


class OverviewMetric(BaseModel):
    label: str
    value: float | int | str
    change_yoy_pct: Optional[float] = None


class OverviewResponse(BaseModel):
    period_start: date
    period_end: date
    metrics: List[OverviewMetric]
    top_ssic: Optional[Dict[str, str | int]] = None
    hottest_planning_area: Optional[Dict[str, str | int]] = None


class TrendPoint(BaseModel):
    month: int = Field(..., description="YYYYMM")
    count: int


class TrendsResponse(BaseModel):
    series: List[TrendPoint]


class RankingItem(BaseModel):
    ssic_code: Optional[str]
    ssic_description: Optional[str]
    count: int


class RankingsResponse(BaseModel):
    items: List[RankingItem]


class MapHotspot(BaseModel):
    subzone_id: Optional[str]
    name: Optional[str]
    planning_area_id: Optional[str]
    count: int
    geometry: str


class MapHotspotsResponse(BaseModel):
    month_start: int
    month_end: int
    hotspots: List[MapHotspot]


class EntitySummary(BaseModel):
    uen: str
    entity_name: str
    entity_status_description: str
    entity_type_description: str
    business_constitution_description: str
    company_type_description: Optional[str]
    registration_incorporation_date: Optional[date]
    uen_issue_date: Optional[date]
    primary_ssic_code: Optional[str]
    secondary_ssic_code: Optional[str]
    postal_code: Optional[str]
    planning_area_id: Optional[str]
    subzone_id: Optional[str]


class EntitySearchResponse(BaseModel):
    total: int
    items: List[EntitySummary]


class EntityDetailResponse(EntitySummary):
    primary_ssic_norm: Optional[str]
    secondary_ssic_norm: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]


ALLOWED_AREA_TYPES = {"planning_area", "subzone"}

@lru_cache
def load_ssic_categories() -> dict:
    data_path = Path(__file__).resolve().parent.parent / "data" / "ssic_categories.json"
    if not data_path.exists():
        return {"sectors": [], "categories": []}
    return json.loads(data_path.read_text(encoding="utf-8"))


def resolve_ssic_patterns(ssic: Optional[str], ssic_category: Optional[str]) -> list[str]:
    if ssic:
        cleaned = ssic.strip()
        if cleaned == "":
            return []
        if not cleaned.isdigit():
            raise HTTPException(status_code=400, detail="ssic must be digits")
        return [cleaned]
    if ssic_category:
        data = load_ssic_categories()
        for category in data.get("categories", []):
            if category.get("id") == ssic_category:
                return [str(code) for code in category.get("ssic", [])]
        raise HTTPException(status_code=400, detail="Unknown ssic_category")
    return []


def parse_date(value: Optional[str], default: date) -> date:
    if not value:
        return default
    try:
        return datetime.fromisoformat(value).date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format; use YYYY-MM-DD") from exc


def to_yyyymm(value: date) -> int:
    return value.year * 100 + value.month


def build_ssic_filter(
    ssic: Optional[str],
    ssic_category: Optional[str],
    column: str = "primary_ssic_norm",
) -> tuple[str, Dict[str, object]]:
    patterns = resolve_ssic_patterns(ssic, ssic_category)
    if not patterns:
        return "", {}

    clauses = []
    params: Dict[str, object] = {}
    for idx, pattern in enumerate(patterns):
        key = f"ssic_{idx}"
        if pattern.endswith("*"):
            value = pattern[:-1]
            clauses.append(f"startsWith({column}, {{{key}:String}})")
            params[key] = value
        else:
            clauses.append(f"{column} = {{{key}:String}}")
            params[key] = pattern
    return " AND (" + " OR ".join(clauses) + ")", params


def build_area_filter(area: Optional[str], area_type: str) -> tuple[str, Dict[str, object]]:
    if not area:
        return "", {}
    if area_type not in ALLOWED_AREA_TYPES:
        raise HTTPException(status_code=400, detail="area_type must be planning_area or subzone")
    column = "planning_area_id" if area_type == "planning_area" else "subzone_id"
    return f" AND {column} = {{area:String}}", {"area": area}


@router.get("/ssic/categories")

def get_ssic_categories():
    return load_ssic_categories()


@router.get("/overview", response_model=OverviewResponse)
def get_overview(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    ssic: Optional[str] = None,
    ssic_category: Optional[str] = Query(None, alias="ssic_category"),
    area: Optional[str] = None,
    area_type: str = Query("planning_area"),
):
    client = get_client()
    end_date = parse_date(to_date, date.today())
    start_date = parse_date(from_date, end_date - timedelta(days=30))
    prev_start = start_date.replace(year=start_date.year - 1)
    prev_end = end_date.replace(year=end_date.year - 1)

    ssic_clause, ssic_params = build_ssic_filter(ssic, ssic_category)
    area_clause, area_params = build_area_filter(area, area_type)

    total_sql = (
        "SELECT count() FROM acra_entities_enriched "
        "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        + ssic_clause
        + area_clause
    )
    params = {"start": start_date, "end": end_date}
    params.update(ssic_params)
    params.update(area_params)
    total = client.query(total_sql, parameters=params).result_rows[0][0]

    prev_params = {"start": prev_start, "end": prev_end}
    prev_params.update(ssic_params)
    prev_params.update(area_params)
    prev_total = client.query(total_sql, parameters=prev_params).result_rows[0][0]

    yoy = None
    if prev_total:
        yoy = round((total - prev_total) / prev_total * 100, 2)

    top_ssic_sql = (
        "SELECT primary_ssic_norm, count() AS cnt "
        "FROM acra_entities_enriched "
        "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32} "
        + ssic_clause
        + area_clause
        + " GROUP BY primary_ssic_norm "
        "ORDER BY cnt DESC "
        "LIMIT 1"
    )
    top_ssic_row = client.query(top_ssic_sql, parameters=params).result_rows
    top_ssic = None
    if top_ssic_row and top_ssic_row[0][0] is not None:
        top_ssic = {"ssic_code": top_ssic_row[0][0], "count": top_ssic_row[0][1]}

    top_area_sql = (
        "SELECT planning_area_id, count() AS cnt "
        "FROM acra_entities_enriched "
        "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32} "
        + ssic_clause
        + " GROUP BY planning_area_id "
        "ORDER BY cnt DESC "
        "LIMIT 1"
    )
    top_area_row = client.query(top_area_sql, parameters=params).result_rows
    hottest = None
    if top_area_row and top_area_row[0][0] is not None:
        hottest = {
            "planning_area_id": top_area_row[0][0],
            "count": top_area_row[0][1],
            "area_type": "planning_area",
        }
    else:
        fallback_sql = (
            "SELECT subzone_id, count() AS cnt "
            "FROM acra_entities_enriched "
            "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32} "
            + ssic_clause
            + " GROUP BY subzone_id "
            "ORDER BY cnt DESC "
            "LIMIT 1"
        )
        fallback_row = client.query(fallback_sql, parameters=params).result_rows
        if fallback_row and fallback_row[0][0] is not None:
            hottest = {
                "planning_area_id": fallback_row[0][0],
                "count": fallback_row[0][1],
                "area_type": "subzone",
            }
        else:
            hottest = {
                "planning_area_id": "UNMAPPED",
                "count": total,
                "area_type": "unmapped",
            }

    return OverviewResponse(
        period_start=start_date,
        period_end=end_date,
        metrics=[OverviewMetric(label="new_entities_30d", value=total, change_yoy_pct=yoy)],
        top_ssic=top_ssic,
        hottest_planning_area=hottest,
    )


@router.get("/trends/new-entities", response_model=TrendsResponse)
def get_trends(
    ssic: Optional[str] = None,
    ssic_category: Optional[str] = Query(None, alias="ssic_category"),
    area: Optional[str] = None,
    area_type: str = Query("planning_area"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
):
    client = get_client()
    end_date = parse_date(to_date, date.today())
    start_date = parse_date(from_date, end_date - timedelta(days=730))

    ssic_clause, ssic_params = build_ssic_filter(ssic, ssic_category)
    area_clause, area_params = build_area_filter(area, area_type)

    sql = (
        "SELECT toYYYYMM(registration_incorporation_date) AS month, count() AS cnt "
        "FROM acra_entities_enriched "
        "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        + ssic_clause
        + area_clause
        + " GROUP BY month ORDER BY month"
    )

    params = {"start": start_date, "end": end_date}
    params.update(ssic_params)
    params.update(area_params)

    rows = client.query(sql, parameters=params).result_rows
    series = [TrendPoint(month=row[0], count=row[1]) for row in rows]
    return TrendsResponse(series=series)


@router.get("/rankings/top-ssic", response_model=RankingsResponse)
def get_top_ssic(
    ssic: Optional[str] = None,
    ssic_category: Optional[str] = Query(None, alias="ssic_category"),
    area: Optional[str] = None,
    area_type: str = Query("planning_area"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    limit: int = 10,
):
    client = get_client()
    end_date = parse_date(to_date, date.today())
    start_date = parse_date(from_date, end_date - timedelta(days=365))

    ssic_clause, ssic_params = build_ssic_filter(ssic, ssic_category)
    area_clause, area_params = build_area_filter(area, area_type)

    sql = (
        "SELECT primary_ssic_norm, count() AS cnt "
        "FROM acra_entities_enriched "
        "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        + ssic_clause
        + area_clause
        + " GROUP BY primary_ssic_norm ORDER BY cnt DESC LIMIT {limit:UInt32}"
    )

    params = {"start": start_date, "end": end_date, "limit": limit}
    params.update(ssic_params)
    params.update(area_params)

    rows = client.query(sql, parameters=params).result_rows

    items = []
    for ssic_code, cnt in rows:
        desc = None
        if ssic_code:
            desc_rows = client.query(
                "SELECT ssic_description FROM dim_ssic WHERE ssic_code = {code:String} LIMIT 1",
                parameters={"code": ssic_code},
            ).result_rows
            if desc_rows:
                desc = desc_rows[0][0]
        items.append(RankingItem(ssic_code=ssic_code, ssic_description=desc, count=cnt))

    return RankingsResponse(items=items)


@router.get("/map/hotspots", response_model=MapHotspotsResponse)
def map_hotspots(
    ssic: Optional[str] = None,
    ssic_category: Optional[str] = Query(None, alias="ssic_category"),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
):
    client = get_client()
    end_date = parse_date(to_date, date.today())
    start_date = parse_date(from_date, end_date - timedelta(days=365))

    start_month = to_yyyymm(start_date)
    end_month = to_yyyymm(end_date)

    ssic_clause, ssic_params = build_ssic_filter(ssic, ssic_category)

    sql = (
        "SELECT s.subzone_id, s.name, s.planning_area_id, s.geometry, count() AS cnt "
        "FROM acra_entities_enriched AS e "
        "LEFT JOIN dim_subzone AS s ON e.subzone_id = s.subzone_id "
        "WHERE e.registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        + ssic_clause.replace("primary_ssic_norm", "e.primary_ssic_norm")
        + " AND s.geometry IS NOT NULL AND s.geometry != '' "
        + " GROUP BY s.subzone_id, s.name, s.planning_area_id, s.geometry "
        "ORDER BY cnt DESC"
    )

    params = {"start": start_date, "end": end_date}
    params.update(ssic_params)

    rows = client.query(sql, parameters=params).result_rows
    hotspots = [
        MapHotspot(
            subzone_id=row[0],
            name=row[1],
            planning_area_id=row[2],
            geometry=row[3],
            count=row[4],
        )
        for row in rows
    ]

    return MapHotspotsResponse(month_start=start_month, month_end=end_month, hotspots=hotspots)


@router.get("/entities/search", response_model=EntitySearchResponse)
def search_entities(
    q: Optional[str] = None,
    ssic: Optional[str] = None,
    ssic_category: Optional[str] = Query(None, alias="ssic_category"),
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    client = get_client()

    where = "WHERE 1=1"
    params: Dict[str, object] = {"limit": limit, "offset": offset}

    if q:
        where += " AND (positionCaseInsensitive(entity_name, {q:String}) > 0 OR uen = {q:String})"
        params["q"] = q

    if status:
        where += " AND entity_status_description = {status:String}"
        params["status"] = status

    ssic_clause, ssic_params = build_ssic_filter(ssic, ssic_category)
    where += ssic_clause
    params.update(ssic_params)

    total = client.query(
        f"SELECT count() FROM acra_entities_enriched {where}", parameters=params
    ).result_rows[0][0]

    sql = (
        "SELECT uen, entity_name, entity_status_description, entity_type_description, "
        "business_constitution_description, company_type_description, "
        "registration_incorporation_date, uen_issue_date, primary_ssic_code, "
        "secondary_ssic_code, postal_code, planning_area_id, subzone_id "
        "FROM acra_entities_enriched "
        + where
        + " ORDER BY registration_incorporation_date DESC NULLS LAST "
        + " LIMIT {limit:UInt32} OFFSET {offset:UInt32}"
    )

    rows = client.query(sql, parameters=params).result_rows
    items = [
        EntitySummary(
            uen=row[0],
            entity_name=row[1],
            entity_status_description=row[2],
            entity_type_description=row[3],
            business_constitution_description=row[4],
            company_type_description=row[5],
            registration_incorporation_date=row[6],
            uen_issue_date=row[7],
            primary_ssic_code=row[8],
            secondary_ssic_code=row[9],
            postal_code=row[10],
            planning_area_id=row[11],
            subzone_id=row[12],
        )
        for row in rows
    ]

    return EntitySearchResponse(total=total, items=items)


@router.get("/entities/{uen}", response_model=EntityDetailResponse)
def get_entity(uen: str):
    client = get_client()
    sql = (
        "SELECT uen, entity_name, entity_status_description, entity_type_description, "
        "business_constitution_description, company_type_description, registration_incorporation_date, "
        "uen_issue_date, primary_ssic_code, secondary_ssic_code, primary_ssic_norm, secondary_ssic_norm, "
        "postal_code, planning_area_id, subzone_id, latitude, longitude "
        "FROM acra_entities_enriched WHERE uen = {uen:String} LIMIT 1"
    )
    rows = client.query(sql, parameters={"uen": uen}).result_rows
    if not rows:
        raise HTTPException(status_code=404, detail="UEN not found")

    row = rows[0]
    return EntityDetailResponse(
        uen=row[0],
        entity_name=row[1],
        entity_status_description=row[2],
        entity_type_description=row[3],
        business_constitution_description=row[4],
        company_type_description=row[5],
        registration_incorporation_date=row[6],
        uen_issue_date=row[7],
        primary_ssic_code=row[8],
        secondary_ssic_code=row[9],
        primary_ssic_norm=row[10],
        secondary_ssic_norm=row[11],
        postal_code=row[12],
        planning_area_id=row[13],
        subzone_id=row[14],
        latitude=row[15],
        longitude=row[16],
    )
