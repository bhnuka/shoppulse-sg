from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.db import get_client
from app.core.nl2sql import Intent, build_slots, build_sql, classify_intent, summarize

router = APIRouter(prefix="/api/chat")


class ChatRequest(BaseModel):
    question: str


class ChatSqlResponse(BaseModel):
    intent: Intent
    slots: Dict[str, Any]
    sql: str


class ChatResponse(ChatSqlResponse):
    data: List[Dict[str, Any]]
    narrative: str


def execute(sql: str, params: Dict[str, Any], columns: List[str]) -> List[Dict[str, Any]]:
    client = get_client()
    rows = client.query(sql, parameters=params).result_rows
    return [dict(zip(columns, row)) for row in rows]


@router.post("/sql-only", response_model=ChatSqlResponse)
def sql_only(payload: ChatRequest):
    intent = classify_intent(payload.question)
    slots = build_slots(intent, payload.question)
    sql_payload = build_sql(intent, slots)
    return ChatSqlResponse(intent=intent, slots=slots.model_dump(), sql=sql_payload.sql)


@router.post("/query", response_model=ChatResponse)
def query(payload: ChatRequest):
    intent = classify_intent(payload.question)
    slots = build_slots(intent, payload.question)
    sql_payload = build_sql(intent, slots)
    data = execute(sql_payload.sql, sql_payload.params, sql_payload.columns)
    narrative = summarize(intent, slots, data)
    return ChatResponse(
        intent=intent,
        slots=slots.model_dump(),
        sql=sql_payload.sql,
        data=data,
        narrative=narrative,
    )
