from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException
from pydantic import BaseModel, Field


class Intent(str, Enum):
    NEW_ENTITIES_TREND = "NEW_ENTITIES_TREND"
    TOP_INDUSTRIES_IN_AREA = "TOP_INDUSTRIES_IN_AREA"
    HOTTEST_AREAS_FOR_INDUSTRY = "HOTTEST_AREAS_FOR_INDUSTRY"
    COMPARE_AREAS = "COMPARE_AREAS"
    ENTITY_LOOKUP = "ENTITY_LOOKUP"
    EXPLAIN_CHANGE = "EXPLAIN_CHANGE"


INDUSTRY_KEYWORDS = {
    "f&b": "56",
    "food": "56",
    "restaurant": "56",
    "cafe": "56",
    "retail": "47",
    "wholesale": "46",
    "finance": "64",
    "bank": "64",
    "insurance": "65",
    "software": "62",
    "it": "62",
    "tech": "62",
}


class SlotBase(BaseModel):
    start_date: date
    end_date: date
    ssic: Optional[str] = None


class TrendSlots(SlotBase):
    area: Optional[str] = None


class TopIndustriesSlots(SlotBase):
    area: Optional[str] = None


class HottestAreasSlots(SlotBase):
    pass


class CompareAreasSlots(SlotBase):
    areas: List[str] = Field(default_factory=list)


class EntityLookupSlots(BaseModel):
    uen: str


class ExplainChangeSlots(SlotBase):
    area: Optional[str] = None


@dataclass
class SqlPayload:
    sql: str
    params: Dict[str, object]
    columns: List[str]


def clamp_date_range(start: date, end: date, max_years: int = 10) -> Tuple[date, date]:
    if start > end:
        start, end = end, start
    max_span = timedelta(days=365 * max_years)
    if end - start > max_span:
        start = end - max_span
    return start, end


def default_range(months: int) -> Tuple[date, date]:
    end = date.today()
    start = end - timedelta(days=30 * months)
    return start, end


def extract_years(question: str) -> Optional[Tuple[date, date]]:
    years = [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", question)]
    if not years:
        return None
    if len(years) == 1:
        return date(years[0], 1, 1), date(years[0], 12, 31)
    years.sort()
    return date(years[0], 1, 1), date(years[-1], 12, 31)


def extract_relative_range(question: str) -> Optional[Tuple[date, date]]:
    text = question.lower()
    end = date.today()
    if "last 12 months" in text or "past 12 months" in text or "last year" in text:
        return end - timedelta(days=365), end
    if "last 6 months" in text:
        return end - timedelta(days=180), end
    if "last 3 months" in text:
        return end - timedelta(days=90), end
    if "last 24 months" in text:
        return end - timedelta(days=730), end
    return None


def extract_area(question: str) -> Optional[str]:
    match = re.search(r"\b(in|for)\s+([A-Za-z ]+?)(?:\s+last|\s+since|\s+from|\s+to|\s+\d{4}|$)", question, re.IGNORECASE)
    return match.group(2).strip() if match else None


def extract_compare_areas(question: str) -> List[str]:
    match = re.search(r"compare\s+(.+?)\s+vs\s+(.+?)(?:\s+for|\s+last|\s+since|\s+\d{4}|$)", question, re.IGNORECASE)
    if match:
        return [match.group(1).strip(), match.group(2).strip()]
    match = re.search(r"(.+?)\s+vs\s+(.+?)(?:\s+for|\s+last|\s+since|\s+\d{4}|$)", question, re.IGNORECASE)
    if match:
        return [match.group(1).strip(), match.group(2).strip()]
    return []


def extract_uen(question: str) -> Optional[str]:
    match = re.search(r"\b(\d{8}[A-Z])\b", question, re.IGNORECASE)
    return match.group(1).upper() if match else None


def extract_ssic(question: str) -> Optional[str]:
    match = re.search(r"\bssic\s*(\d{2,5})\b", question, re.IGNORECASE)
    return match.group(1) if match else None


def match_industry_keyword(question: str) -> Optional[str]:
    text = question.lower()
    for keyword, ssic in INDUSTRY_KEYWORDS.items():
        if keyword in text:
            return ssic
    return None


def classify_intent(question: str) -> Intent:
    text = question.lower()
    if "uen" in text or extract_uen(question):
        return Intent.ENTITY_LOOKUP
    if "compare" in text or " vs " in text:
        return Intent.COMPARE_AREAS
    if "top" in text and ("ssic" in text or "industry" in text or "industries" in text):
        return Intent.TOP_INDUSTRIES_IN_AREA
    if "where" in text and ("cluster" in text or "hot" in text or "hottest" in text):
        return Intent.HOTTEST_AREAS_FOR_INDUSTRY
    if "why" in text or "spike" in text:
        return Intent.EXPLAIN_CHANGE
    return Intent.NEW_ENTITIES_TREND


def build_slots(intent: Intent, question: str) -> BaseModel:
    date_range = extract_relative_range(question) or extract_years(question) or default_range(24)
    start, end = clamp_date_range(*date_range)
    ssic = extract_ssic(question) or match_industry_keyword(question)

    if intent == Intent.ENTITY_LOOKUP:
        uen = extract_uen(question)
        if not uen:
            raise HTTPException(status_code=400, detail="UEN not found in question")
        return EntityLookupSlots(uen=uen)

    if intent == Intent.COMPARE_AREAS:
        areas = extract_compare_areas(question)
        if not areas:
            raise HTTPException(status_code=400, detail="Provide two areas to compare")
        return CompareAreasSlots(start_date=start, end_date=end, ssic=ssic, areas=areas)

    if intent == Intent.TOP_INDUSTRIES_IN_AREA:
        return TopIndustriesSlots(start_date=start, end_date=end, ssic=None, area=extract_area(question))

    if intent == Intent.HOTTEST_AREAS_FOR_INDUSTRY:
        return HottestAreasSlots(start_date=start, end_date=end, ssic=ssic)

    if intent == Intent.EXPLAIN_CHANGE:
        return ExplainChangeSlots(start_date=start, end_date=end, ssic=ssic, area=extract_area(question))

    return TrendSlots(start_date=start, end_date=end, ssic=ssic, area=extract_area(question))


def build_sql(intent: Intent, slots: BaseModel) -> SqlPayload:
    if intent == Intent.ENTITY_LOOKUP:
        slots = slots  # type: ignore[assignment]
        sql = (
            "SELECT uen, entity_name, entity_status_description, entity_type_description, "
            "business_constitution_description, company_type_description, registration_incorporation_date, "
            "uen_issue_date, primary_ssic_code, secondary_ssic_code, primary_ssic_norm, secondary_ssic_norm, "
            "postal_code, planning_area_id, subzone_id, latitude, longitude "
            "FROM acra_entities_enriched WHERE uen = {uen:String} LIMIT 1"
        )
        return SqlPayload(
            sql=sql,
            params={"uen": slots.uen},
            columns=[
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
                "primary_ssic_norm",
                "secondary_ssic_norm",
                "postal_code",
                "planning_area_id",
                "subzone_id",
                "latitude",
                "longitude",
            ],
        )

    if isinstance(slots, EntityLookupSlots):
        raise HTTPException(status_code=400, detail="Invalid slots for intent")

    start = slots.start_date
    end = slots.end_date

    if intent == Intent.NEW_ENTITIES_TREND:
        sql = (
            "SELECT toYYYYMM(registration_incorporation_date) AS month, count() AS cnt "
            "FROM acra_entities_enriched "
            "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        )
        params: Dict[str, object] = {"start": start, "end": end}
        if isinstance(slots, TrendSlots) and slots.area:
            sql += " AND planning_area_id = {area:String}"
            params["area"] = slots.area
        if slots.ssic:
            sql += " AND startsWith(primary_ssic_norm, {ssic:String})"
            params["ssic"] = slots.ssic
        sql += " GROUP BY month ORDER BY month"
        return SqlPayload(sql=sql, params=params, columns=["month", "count"])

    if intent == Intent.TOP_INDUSTRIES_IN_AREA:
        sql = (
            "SELECT primary_ssic_norm, count() AS cnt "
            "FROM acra_entities_enriched "
            "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        )
        params = {"start": start, "end": end}
        if isinstance(slots, TopIndustriesSlots) and slots.area:
            sql += " AND planning_area_id = {area:String}"
            params["area"] = slots.area
        sql += " GROUP BY primary_ssic_norm ORDER BY cnt DESC LIMIT 10"
        return SqlPayload(sql=sql, params=params, columns=["ssic_code", "count"])

    if intent == Intent.HOTTEST_AREAS_FOR_INDUSTRY:
        sql = (
            "SELECT planning_area_id, count() AS cnt "
            "FROM acra_entities_enriched "
            "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        )
        params = {"start": start, "end": end}
        if slots.ssic:
            sql += " AND startsWith(primary_ssic_norm, {ssic:String})"
            params["ssic"] = slots.ssic
        sql += " GROUP BY planning_area_id ORDER BY cnt DESC LIMIT 10"
        return SqlPayload(sql=sql, params=params, columns=["planning_area_id", "count"])

    if intent == Intent.COMPARE_AREAS:
        slots = slots  # type: ignore[assignment]
        sql = (
            "SELECT toYYYYMM(registration_incorporation_date) AS month, planning_area_id, count() AS cnt "
            "FROM acra_entities_enriched "
            "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32} "
            "AND planning_area_id IN {areas:Array(String)}"
        )
        params = {"start": start, "end": end, "areas": slots.areas}
        if slots.ssic:
            sql += " AND startsWith(primary_ssic_norm, {ssic:String})"
            params["ssic"] = slots.ssic
        sql += " GROUP BY month, planning_area_id ORDER BY month, planning_area_id"
        return SqlPayload(sql=sql, params=params, columns=["month", "planning_area_id", "count"])

    if intent == Intent.EXPLAIN_CHANGE:
        sql = (
            "SELECT toYYYYMM(registration_incorporation_date) AS month, count() AS cnt "
            "FROM acra_entities_enriched "
            "WHERE registration_incorporation_date BETWEEN {start:Date32} AND {end:Date32}"
        )
        params = {"start": start, "end": end}
        if isinstance(slots, ExplainChangeSlots) and slots.area:
            sql += " AND planning_area_id = {area:String}"
            params["area"] = slots.area
        if slots.ssic:
            sql += " AND startsWith(primary_ssic_norm, {ssic:String})"
            params["ssic"] = slots.ssic
        sql += " GROUP BY month ORDER BY month"
        return SqlPayload(sql=sql, params=params, columns=["month", "count"])

    raise HTTPException(status_code=400, detail="Unsupported intent")


def summarize(intent: Intent, slots: BaseModel, rows: List[Dict[str, object]]) -> str:
    if intent == Intent.ENTITY_LOOKUP:
        if not rows:
            return "No entity found for that UEN."
        name = rows[0].get("entity_name")
        status = rows[0].get("entity_status_description")
        return f"Found {name} ({status})."

    if not rows:
        return "No results for the selected filters."

    if intent == Intent.TOP_INDUSTRIES_IN_AREA:
        top = rows[0]
        return f"Top SSIC: {top.get('ssic_code')} with {top.get('count')} new entities."

    if intent == Intent.HOTTEST_AREAS_FOR_INDUSTRY:
        top = rows[0]
        return f"Hottest planning area: {top.get('planning_area_id')} with {top.get('count')} new entities."

    if intent == Intent.COMPARE_AREAS:
        return "Compared areas across the selected period."

    if intent == Intent.EXPLAIN_CHANGE:
        return "Trend data prepared; use month-by-month view to spot spikes."

    return "Trend generated."
