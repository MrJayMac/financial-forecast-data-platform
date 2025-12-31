from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from platform_common.db import Base


class EventRaw(Base):
    __tablename__ = "events_raw"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    event_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    customer_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    region: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    s3_key: Mapped[str | None] = mapped_column(String(512), nullable=True, unique=True)

    is_late: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    inserted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EventQuarantine(Base):
    __tablename__ = "events_quarantine"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    event_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    customer_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    region: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    issues: Mapped[str] = mapped_column(Text, nullable=False)

    inserted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_quarantine_event_id"),
    )
