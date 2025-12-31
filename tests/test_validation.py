from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from ingestion.app.service import process_event
from ingestion.app.models import EventQuarantine
from platform_common.db import Base


def make_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return SessionLocal()


def test_quarantine_on_validation_error(monkeypatch):
    # Patch S3 to avoid network
    monkeypatch.setattr("platform_common.s3.put_json", lambda *args, **kwargs: True)

    session = make_session()

    payload = {
        "event_id": "evt-bad-1",
        # missing event_time
        "customer_id": "cust-42",
        "region": "us-east",
        "amount": -10.0,  # invalid (negative)
        "currency": "USD",
    }

    res = process_event(session, "payment", payload)
    assert res.status == "quarantined"

    eq = session.scalar(select(EventQuarantine).where(EventQuarantine.event_id == "evt-bad-1"))
    assert eq is not None
    assert "validation_error" in (eq.issues or "")
