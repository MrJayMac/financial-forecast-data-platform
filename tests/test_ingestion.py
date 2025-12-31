from __future__ import annotations

from datetime import datetime, timezone, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ingestion.app.service import process_event
from ingestion.app.models import EventRaw, EventQuarantine
from ingestion.app.schemas import EventType
from platform_common.db import Base


class DummyS3:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def put_json(self, key: str, data: dict) -> bool:
        self.keys.append(key)
        return True


def make_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return SessionLocal()


def test_process_event_accept_and_duplicate(monkeypatch):
    # Patch S3 uploader
    dummy = DummyS3()
    monkeypatch.setattr("platform_common.s3.put_json", dummy.put_json)

    session = make_session()

    payload = {
        "event_id": "evt-1",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": "cust-1",
        "region": "us-east",
        "amount": 100.0,
        "currency": "USD",
    }

    res1 = process_event(session, "payment", payload)
    assert res1.status == "accepted"
    assert res1.s3_key is not None

    res2 = process_event(session, "payment", payload)
    assert res2.status == "duplicate"


def test_process_event_late_arrival(monkeypatch):
    dummy = DummyS3()
    monkeypatch.setattr("platform_common.s3.put_json", dummy.put_json)

    session = make_session()

    old_time = datetime.now(timezone.utc) - timedelta(days=10)
    payload = {
        "event_id": "evt-late",
        "event_time": old_time.isoformat(),
        "customer_id": "cust-1",
        "region": "us-east",
        "amount": 5.0,
        "currency": "USD",
    }

    res = process_event(session, "payment", payload)
    assert res.status == "accepted"
    assert res.is_late is True
