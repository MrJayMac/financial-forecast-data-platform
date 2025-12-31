from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from statsmodels.tsa.statespace.sarimax import SARIMAX

from platform_common.db import get_engine, session_scope
from platform_common.db import Base
from forecasting.models import (
    ModelRun,
    ForecastRevenueDaily,
    ForecastSubscriptionsDaily,
)


def _fit_and_forecast(series: pd.Series, horizon: int = 30) -> Tuple[pd.Series, pd.DataFrame]:
    # Simple baseline SARIMAX with weekly seasonality
    model = SARIMAX(series, order=(1, 1, 1), seasonal_order=(1, 0, 1, 7), enforce_stationarity=False, enforce_invertibility=False)
    results = model.fit(disp=False)
    forecast_res = results.get_forecast(steps=horizon)
    yhat = forecast_res.predicted_mean
    ci = forecast_res.conf_int(alpha=0.2)  # 80% interval
    return yhat, ci


def _upsert_model_run(session: Session, target: str, train_start: date, train_end: date) -> int:
    mr = ModelRun(target=target, model_name="SARIMAX(1,1,1)(1,0,1,7)", params={"alpha": 0.2}, train_start=train_start, train_end=train_end)
    session.add(mr)
    session.flush()
    return mr.id


def forecast_revenue_daily(horizon: int = 30) -> int:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        df = pd.read_sql(text("select date_key::date as date_key, coalesce(revenue_amount,0)::numeric as revenue_amount from fact_revenue_daily order by 1"), conn)
    if df.empty:
        return 0

    series = df.set_index("date_key")["revenue_amount"].astype(float)
    yhat, ci = _fit_and_forecast(series, horizon)

    with session_scope() as session:
        run_id = _upsert_model_run(session, target="revenue_daily", train_start=series.index.min(), train_end=series.index.max())
        for d, y in yhat.items():
            lower = float(ci.loc[d, "lower revenue_amount"]) if "lower revenue_amount" in ci.columns else float(y * 0.9)
            upper = float(ci.loc[d, "upper revenue_amount"]) if "upper revenue_amount" in ci.columns else float(y * 1.1)
            row = ForecastRevenueDaily(run_id=run_id, date_key=pd.to_datetime(d).date(), yhat=float(y), yhat_lower=lower, yhat_upper=upper)
            session.add(row)
    return len(yhat)


def forecast_subscriptions_daily(horizon: int = 30) -> int:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        df = pd.read_sql(text("select date_key::date as date_key, coalesce(active_subscriptions,0)::int as y from fact_subscriptions_snapshot order by 1"), conn)
    if df.empty:
        return 0

    series = df.set_index("date_key")["y"].astype(float)
    yhat, ci = _fit_and_forecast(series, horizon)

    with session_scope() as session:
        run_id = _upsert_model_run(session, target="subscriptions_daily", train_start=series.index.min(), train_end=series.index.max())
        for d, y in yhat.items():
            lower = float(ci.loc[d, "lower y"]) if "lower y" in ci.columns else float(y * 0.9)
            upper = float(ci.loc[d, "upper y"]) if "upper y" in ci.columns else float(y * 1.1)
            row = ForecastSubscriptionsDaily(run_id=run_id, date_key=pd.to_datetime(d).date(), yhat=float(y), yhat_lower=lower, yhat_upper=upper)
            session.add(row)
    return len(yhat)


if __name__ == "__main__":
    n1 = forecast_revenue_daily()
    n2 = forecast_subscriptions_daily()
    print({"revenue_forecasts": n1, "subscriptions_forecasts": n2})
