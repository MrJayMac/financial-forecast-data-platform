from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Integer, Numeric, String, JSON, func
from sqlalchemy.orm import Mapped, mapped_column

from platform_common.db import Base


class ModelRun(Base):
    __tablename__ = "model_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    target: Mapped[str] = mapped_column(String(64), nullable=False)  # e.g., revenue_daily, subscriptions_daily
    model_name: Mapped[str] = mapped_column(String(128), nullable=False)
    params: Mapped[dict] = mapped_column(JSON, nullable=False)
    train_start: Mapped[date] = mapped_column(Date, nullable=False)
    train_end: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ForecastRevenueDaily(Base):
    __tablename__ = "forecast_revenue_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_key: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    yhat: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    yhat_lower: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    yhat_upper: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ForecastSubscriptionsDaily(Base):
    __tablename__ = "forecast_subscriptions_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_key: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    yhat: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    yhat_lower: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    yhat_upper: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
