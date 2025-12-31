from __future__ import annotations

from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse
from loguru import logger

from platform_common.db import Base, get_engine, session_scope
from platform_common.s3 import ensure_bucket
from platform_common.config import settings

from .schemas import BatchIngestionResponse, EventType, IngestionResult
from .service import process_event
from .models import EventRaw, EventQuarantine

app = FastAPI(title="FFDP Ingestion API", version="0.1.0")


@app.on_event("startup")
def on_startup() -> None:
    logger.add(lambda msg: print(msg, end=""))
    logger.info("Starting up: creating tables and ensuring bucket")
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    ensure_bucket()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ingest/{event_type}", response_model=IngestionResult)
def ingest_event(event_type: EventType = Path(...), payload: dict[str, Any] | None = None):
    if payload is None:
        raise HTTPException(status_code=400, detail="Missing JSON body")

    with session_scope() as session:
        try:
            result = process_event(session, event_type, payload)
            return JSONResponse(status_code=202, content=result.model_dump())
        except Exception as e:
            logger.exception("Failed to ingest event: {}", e)
            raise HTTPException(status_code=500, detail="Internal error")


@app.post("/ingest/{event_type}/batch", response_model=BatchIngestionResponse)
def ingest_batch(event_type: EventType = Path(...), payloads: list[dict[str, Any]] | None = None):
    if not payloads:
        raise HTTPException(status_code=400, detail="Empty batch")

    accepted = 0
    duplicates = 0
    quarantined = 0
    results: list[IngestionResult] = []

    with session_scope() as session:
        for payload in payloads:
            try:
                result = process_event(session, event_type, payload)
                if result.status == "accepted":
                    accepted += 1
                elif result.status == "duplicate":
                    duplicates += 1
                elif result.status == "quarantined":
                    quarantined += 1
                results.append(result)
            except Exception as e:
                logger.exception("Batch item failed: {}", e)
                quarantined += 1
                results.append(IngestionResult(status="quarantined", event_id=payload.get("event_id", "unknown"), event_type=event_type, issues=["exception"], is_late=False))

    return BatchIngestionResponse(accepted=accepted, duplicates=duplicates, quarantined=quarantined, results=results)
