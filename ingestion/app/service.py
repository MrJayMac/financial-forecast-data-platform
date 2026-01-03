from __future__ import annotations

from datetime import datetime
from typing import Any
import json

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_common.s3 import put_json
from .models import EventRaw, EventQuarantine
from .schemas import EventSchemaMap, EventType, IngestionResult
from .quality import evaluate_quality
from pydantic import ValidationError
from datetime import timezone


def s3_key_for(event_type: str, event_id: str, event_time: datetime) -> str:
    dt = event_time.strftime("%Y-%m-%d")
    return f"raw/{event_type}/dt={dt}/{event_id}.json"


def process_event(session: Session, event_type: EventType, payload: dict[str, Any]) -> IngestionResult:
    # Parse by type with validation; quarantine on failure
    schema_cls = EventSchemaMap[event_type]
    try:
        obj = schema_cls(**payload)
    except ValidationError as ve:
        # Populate minimal required fields for quarantine row
        now = datetime.now(timezone.utc)
        event_id = str(payload.get("event_id", f"invalid-{now.timestamp()}"))
        customer_id = str(payload.get("customer_id", "unknown"))
        region = str(payload.get("region", "unknown"))
        eq = EventQuarantine(
            event_id=event_id,
            event_type=event_type,
            event_time=now,
            customer_id=customer_id,
            region=region,
            payload=payload,
            issues="validation_error: " + "; ".join([e.get("msg", "error") for e in ve.errors()]),
        )
        session.add(eq)
        session.flush()
        return IngestionResult(status="quarantined", event_id=event_id, event_type=event_type, issues=["validation_error"], is_late=False)

    # Duplicate detection across raw and quarantine
    exists_raw = session.scalar(select(EventRaw.id).where(EventRaw.event_id == obj.event_id).limit(1))
    if exists_raw is not None:
        return IngestionResult(status="duplicate", event_id=obj.event_id, event_type=event_type)

    exists_q = session.scalar(select(EventQuarantine.id).where(EventQuarantine.event_id == obj.event_id).limit(1))
    if exists_q is not None:
        return IngestionResult(status="duplicate", event_id=obj.event_id, event_type=event_type)

    # Quality evaluation
    q = evaluate_quality(json.loads(obj.model_dump_json()), event_type)

    if not q.is_valid:
        # Quarantine record
        eq = EventQuarantine(
            event_id=obj.event_id,
            event_type=event_type,
            event_time=obj.event_time,
            customer_id=obj.customer_id,
            region=obj.region,
            payload=json.loads(obj.model_dump_json()),
            issues=",".join(q.issues),
        )
        session.add(eq)
        session.flush()
        return IngestionResult(status="quarantined", event_id=obj.event_id, event_type=event_type, issues=q.issues, is_late=q.is_late)

    # Accepted: write to S3 (idempotent write)
    key = s3_key_for(event_type, obj.event_id, obj.event_time)
    put_json(key, json.loads(obj.model_dump_json()))

    er = EventRaw(
        event_id=obj.event_id,
        event_type=event_type,
        event_time=obj.event_time,
        customer_id=obj.customer_id,
        region=obj.region,
        payload=json.loads(obj.model_dump_json()),
        s3_key=key,
        is_late=q.is_late,
    )
    session.add(er)
    session.flush()

    return IngestionResult(status="accepted", event_id=obj.event_id, event_type=event_type, is_late=q.is_late, s3_key=key)
