from __future__ import annotations

from datetime import datetime
from typing import Any

from prefect import flow, task
from loguru import logger

from platform_common.db import Base, get_engine, session_scope
from transformations.runner import run_all as run_transformations
from forecasting.arima import forecast_revenue_daily, forecast_subscriptions_daily
from ingestion.app.service import process_event
from ingestion.app.schemas import EventType


@task
def transformations_task() -> None:
    logger.info("Running SQL transformations")
    engine = get_engine()
    # Ensure models are registered so Base.metadata knows about them
    from ingestion.app import models as _ingestion_models  # noqa: F401
    from forecasting import models as _forecast_models  # noqa: F401
    Base.metadata.create_all(bind=engine)  # ensure base tables exist
    run_transformations()


@task
def forecast_task() -> dict[str, int]:
    logger.info("Running forecasts")
    n1 = forecast_revenue_daily()
    n2 = forecast_subscriptions_daily()
    logger.info("Forecasted revenue days: {} subscriptions days: {}", n1, n2)
    return {"revenue": n1, "subscriptions": n2}


@task
def batch_ingest_task(events: list[tuple[EventType, dict[str, Any]]]) -> dict[str, int]:
    logger.info("Batch ingesting {} events", len(events))
    from ingestion.app.models import EventRaw, EventQuarantine  # ensure tables registered

    accepted = duplicates = quarantined = 0
    with session_scope() as session:
        for et, payload in events:
            res = process_event(session, et, payload)
            if res.status == "accepted":
                accepted += 1
            elif res.status == "duplicate":
                duplicates += 1
            else:
                quarantined += 1
    logger.info("Batch ingest results: accepted={} duplicates={} quarantined={}", accepted, duplicates, quarantined)
    return {"accepted": accepted, "duplicates": duplicates, "quarantined": quarantined}


@flow(name="daily-transform-and-forecast")
def daily_transform_and_forecast() -> dict[str, int]:
    transformations_task()
    res = forecast_task()
    return res


@flow(name="scheduled-batch-ingestion")
def scheduled_batch_ingestion(events: list[tuple[EventType, dict[str, Any]]]) -> dict[str, int]:
    return batch_ingest_task(events)
