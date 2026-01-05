from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import text

from platform_common.db import get_engine, Base
from transformations.runner import run_all as run_transformations

app = FastAPI(title="FFDP Analytics API", version="0.1.0")


@app.on_event("startup")
def on_startup() -> None:
    # Ensure tables exist and run transformations once to create views
    engine = get_engine()
    # Register models
    from ingestion.app import models as _ingestion_models  # noqa: F401
    from forecasting import models as _forecast_models  # noqa: F401
    Base.metadata.create_all(bind=engine)
    try:
        run_transformations()
    except Exception:
        # Views may depend on data; ignore failures on cold start
        logger.warning("Transformations run failed or not needed on startup")

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return """
    <!doctype html>
    <html lang=\"en\">
    <head>
      <meta charset=\"utf-8\" />
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
      <title>FFDP Dashboard</title>
      <style>
        body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; color: #0f172a; }
        h1 { margin-bottom: 8px; }
        h2 { margin-top: 24px; }
        .card { border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; margin: 12px 0; }
        table { border-collapse: collapse; width: 100%; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb; }
        .muted { color: #64748b; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }
        code.badge { background: #f1f5f9; padding: 2px 6px; border-radius: 6px; }
      </style>
    </head>
    <body>
      <h1>FFDP Dashboard</h1>
      <div class=\"muted\">Simple view of key metrics from the Analytics API.</div>

      <div class=\"grid\">
        <div class=\"card\">
          <h2>MRR</h2>
          <div id=\"mrr\">Loading...</div>
        </div>
        <div class=\"card\">
          <h2>Churn</h2>
          <div id=\"churn\">Loading...</div>
        </div>
      </div>

      <div class=\"card\">
        <h2>Revenue by Region</h2>
        <div class=\"muted\">Latest rows</div>
        <table>
          <thead>
            <tr><th>Date</th><th>Region</th><th>Revenue</th></tr>
          </thead>
          <tbody id=\"rev-rows\"><tr><td colspan=3>Loading...</td></tr></tbody>
        </table>
      </div>

      <div class=\"card\">
        <h2>Forecast vs Actual</h2>
        <div class=\"muted\">Latest rows</div>
        <table>
          <thead>
            <tr><th>Date</th><th>Actual</th><th>Forecast</th><th>Variance</th><th>Variance %</th></tr>
          </thead>
          <tbody id=\"fa-rows\"><tr><td colspan=5>Loading...</td></tr></tbody>
        </table>
      </div>

      <script>
        async function getJSON(path) {
          const r = await fetch(path);
          if (!r.ok) throw new Error('Request failed: ' + r.status);
          return r.json();
        }

        function setText(id, text) { document.getElementById(id).textContent = text; }

        function fmtMoney(v) { return '$' + Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }

        function renderRows(elId, rows, cols) {
          const el = document.getElementById(elId);
          if (!rows || rows.length === 0) { el.innerHTML = '<tr><td colspan=\"' + cols + '\">No data</td></tr>'; return; }
          el.innerHTML = rows.map(r => '<tr>' + r.map(c => '<td>' + c + '</td>').join('') + '</tr>').join('');
        }

        (async () => {
          try {
            const mrr = await getJSON('/metrics/mrr');
            setText('mrr', `Month ${mrr.month}: ${fmtMoney(mrr.mrr)}`);
          } catch (e) { setText('mrr', 'No data'); }

          try {
            const churn = await getJSON('/metrics/churn');
            setText('churn', `Date ${churn.date}: Cancellations ${churn.cancellations}, Prev Active ${churn.prev_active}, Rate ${(churn.churn_rate*100).toFixed(2)}%`);
          } catch (e) { setText('churn', 'No data'); }

          try {
            const rev = await getJSON('/metrics/revenue_by_region');
            const recent = rev.rows.slice(-10);
            renderRows('rev-rows', recent.map(r => [r.date_key, r.region_key, fmtMoney(r.revenue_amount)]), 3);
          } catch (e) { renderRows('rev-rows', [], 3); }

          try {
            const fa = await getJSON('/metrics/forecast_vs_actual');
            const recent = fa.rows.slice(-10);
            renderRows('fa-rows', recent.map(r => [r.date, fmtMoney(r.actual), fmtMoney(r.forecast), fmtMoney(r.variance), (Number(r.variance_pct)*100).toFixed(2)+'%']), 5);
          } catch (e) { renderRows('fa-rows', [], 5); }
        })();
      </script>
    </body>
    </html>
    """


class RevenueByRegionResponse(BaseModel):
    rows: list[dict]


@app.get("/metrics/revenue_by_region", response_model=RevenueByRegionResponse)
def revenue_by_region(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
):
    engine = get_engine()
    where = []
    params: dict[str, object] = {}
    if start_date:
        where.append("date_key >= :start_date")
        params["start_date"] = start_date
    if end_date:
        where.append("date_key <= :end_date")
        params["end_date"] = end_date
    where_sql = (" where " + " and ".join(where)) if where else ""

    sql = f"""
        select date_key::date, region_key, revenue_amount::numeric
        from fact_revenue_daily
        {where_sql}
        order by 1, 2
    """
    with engine.begin() as conn:
        rows = [dict(r._mapping) for r in conn.execute(text(sql), params)]
    return RevenueByRegionResponse(rows=rows)


class MRRResponse(BaseModel):
    month: str
    mrr: float


@app.get("/metrics/mrr", response_model=MRRResponse)
def mrr(
    month: Optional[str] = Query(None, description="YYYY-MM"),
):
    engine = get_engine()
    if month is None:
        # default to current month
        month = datetime.utcnow().strftime("%Y-%m")
    try:
        dt = datetime.strptime(month, "%Y-%m").date().replace(day=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid month format, expected YYYY-MM")

    sql = """
        select coalesce(sum(revenue_amount),0)::numeric as mrr
        from fact_revenue_daily
        where date_trunc('month', date_key) = :month
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"month": dt}).first()
        mrr_value = float(row[0]) if row else 0.0
    return MRRResponse(month=month, mrr=mrr_value)


class ChurnResponse(BaseModel):
    date: date
    cancellations: int
    prev_active: int
    churn_rate: float


@app.get("/metrics/churn", response_model=ChurnResponse)
def churn(
    day: Optional[date] = Query(None),
):
    engine = get_engine()
    if day is None:
        with engine.begin() as conn:
            latest = conn.execute(text("select max(date_key)::date from fact_subscriptions_snapshot")).scalar()
        if latest is None:
            raise HTTPException(status_code=404, detail="No subscription data")
        day = latest

    with engine.begin() as conn:
        prev_day = conn.execute(text("select :d::date - interval '1 day'"), {"d": day}).scalar()
        cancels = conn.execute(
            text("select count(*) from stg_subscription_events where event_date = :d and (payload::jsonb ->> 'action') = 'canceled'"),
            {"d": day},
        ).scalar()
        prev_active = conn.execute(
            text("select active_subscriptions from fact_subscriptions_snapshot where date_key = :d"),
            {"d": prev_day},
        ).scalar()
    cancellations = int(cancels or 0)
    prev_active_i = int(prev_active or 0)
    churn_rate = float(cancellations / prev_active_i) if prev_active_i > 0 else 0.0
    return ChurnResponse(date=day, cancellations=cancellations, prev_active=prev_active_i, churn_rate=churn_rate)


class ForecastVsActualRow(BaseModel):
    date: date
    actual: float
    forecast: float
    variance: float
    variance_pct: float


class ForecastVsActualResponse(BaseModel):
    rows: list[ForecastVsActualRow]


@app.get("/metrics/forecast_vs_actual", response_model=ForecastVsActualResponse)
def forecast_vs_actual(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
):
    engine = get_engine()
    where = []
    params: dict[str, object] = {}
    if start_date:
        where.append("a.date_key >= :start_date")
        params["start_date"] = start_date
    if end_date:
        where.append("a.date_key <= :end_date")
        params["end_date"] = end_date
    where_sql = (" where " + " and ".join(where)) if where else ""

    sql = f"""
        with latest_run as (
            select max(id) as run_id
            from model_runs
            where target = 'revenue_daily'
        )
        select a.date_key::date as date,
               a.revenue_amount::numeric as actual,
               f.yhat::numeric as forecast,
               (a.revenue_amount::numeric - f.yhat::numeric) as variance,
               case when a.revenue_amount::numeric <> 0 then (a.revenue_amount::numeric - f.yhat::numeric)/a.revenue_amount::numeric else 0 end as variance_pct
        from fact_revenue_daily a
        join latest_run r on true
        left join forecast_revenue_daily f on f.run_id = r.run_id and f.date_key = a.date_key
        {where_sql}
        order by 1
    """
    with engine.begin() as conn:
        rows = [
            ForecastVsActualRow(
                date=r[0], actual=float(r[1] or 0), forecast=float(r[2] or 0), variance=float(r[3] or 0), variance_pct=float(r[4] or 0)
            )
            for r in conn.execute(text(sql), params)
        ]
    return ForecastVsActualResponse(rows=rows)
