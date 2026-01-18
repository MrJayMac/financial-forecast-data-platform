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
      <title>Financial Insights & Forecasts</title>
      <style>
        :root { --bg: #f8fafc; --fg: #0f172a; --muted: #64748b; --card: #ffffff; --border: #e5e7eb; --primary: #2563eb; }
        * { box-sizing: border-box; }
        body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 16px; color: var(--fg); background: var(--bg); }
        header { display:flex; flex-wrap: wrap; gap: 12px; justify-content: space-between; align-items: center; margin-bottom: 16px; }
        h1 { margin: 0; font-size: 22px; }
        .controls { display: flex; gap: 8px; align-items: center; }
        input, button, select { border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; background: #fff; }
        button.primary { background: linear-gradient(90deg, var(--primary), #3b82f6); color: #fff; border: none; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
        .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
        .stat .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .06em; }
        .stat .value { font-size: 28px; font-weight: 700; margin-top: 6px; }
        .muted { color: var(--muted); font-size: 12px; }
        table { border-collapse: collapse; width: 100%; font-size: 14px; }
        th, td { text-align: left; padding: 10px; border-bottom: 1px solid var(--border); }
        thead th { background: #f1f5f9; font-weight: 600; }
        tr:hover td { background: #f9fafb; }
        .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; }
        .pill.good { background: #dcfce7; color: #065f46; }
        .pill.bad { background: #fee2e2; color: #7f1d1d; }
        .bar { height: 8px; background: #e2e8f0; border-radius: 999px; overflow: hidden; }
        .bar > b { display:block; height: 100%; background: var(--primary); }
        .footer { display:flex; justify-content: space-between; align-items: center; margin-top: 8px; }
        [data-theme=\"dark\"] { --bg: #0b1220; --fg: #e2e8f0; --muted: #94a3b8; --card: #0f172a; --border: #1f2937; --primary: #60a5fa; }
        .card:hover { box-shadow: 0 10px 24px rgba(0,0,0,.08); }
        button { transition: transform .15s ease, box-shadow .15s ease; }
        button:hover { transform: translateY(-1px); box-shadow: 0 6px 14px rgba(0,0,0,.08); }
        svg.spark { width: 100%; height: 64px; color: var(--primary); }
      </style>
    </head>
    <body>
      <header>
        <div>
          <h1>Financial Insights & Forecasts</h1>
          <div class=\"muted\">Real-time KPIs, trends, and SARIMAX forecasts</div>
        </div>
        <div class=\"controls\">
          <label class=\"muted\">Start</label><input type=\"date\" id=\"startDate\" />
          <label class=\"muted\">End</label><input type=\"date\" id=\"endDate\" />
          <button id=\"apply\" class=\"primary\">Apply</button>
          <button id=\"refresh\">Refresh</button>
          <button id=\"themeToggle\" aria-label=\"Toggle theme\">🌓</button>
        </div>
      </header>

      <div class=\"grid\">
        <div class=\"card stat\">
          <div class=\"label\">MRR</div>
          <div class=\"value\" id=\"mrrValue\">—</div>
          <div class=\"muted\" id=\"mrrMonth\">Month —</div>
        </div>
        <div class=\"card stat\">
          <div class=\"label\">Churn</div>
          <div class=\"value\" id=\"churnRate\">—</div>
          <div class=\"muted\" id=\"churnDetail\">—</div>
        </div>
        <div class=\"card\">
          <div style=\"display:flex; justify-content: space-between; align-items:center; margin-bottom:8px;\">
            <h2 style=\"margin:0; font-size:16px;\">Top Regions</h2>
            <span class=\"muted\" id=\"regionsTotal\">—</span>
          </div>
          <div id=\"regionsList\" class=\"muted\">No data</div>
        </div>
        <div class=\"card\">
          <div style=\"display:flex; justify-content: space-between; align-items:center; margin-bottom:8px;\"><h2 style=\"margin:0; font-size:16px;\">Revenue Trend</h2></div>
          <svg id=\"revSpark\" class=\"spark\" viewBox=\"0 0 100 30\" preserveAspectRatio=\"none\"></svg>
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
        function qs(id) { return document.getElementById(id); }
        function fmtMoney(v) { return '$' + Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
        function fmtPct(v) { return (Number(v || 0) * 100).toFixed(2) + '%'; }
        function setText(id, text) { qs(id).textContent = text; }
        function buildQuery(params) {
          const p = Object.entries(params || {}).filter(([_, v]) => v);
          if (p.length === 0) return '';
          return '?' + p.map(([k,v]) => k + '=' + encodeURIComponent(v)).join('&');
        }
        function renderRows(elId, rows, cols) {
          const el = qs(elId);
          if (!rows || rows.length === 0) { el.innerHTML = '<tr><td colspan=\"' + cols + '\">No data</td></tr>'; return; }
          el.innerHTML = rows.map(r => '<tr>' + r.map(c => '<td>' + c + '</td>').join('') + '</tr>').join('');
        }
        function groupByRegion(rows) {
          const agg = {}; let total = 0;
          for (const r of rows || []) {
            const k = r.region_key; const v = Number(r.revenue_amount || 0);
            agg[k] = (agg[k] || 0) + v; total += v;
          }
          const arr = Object.entries(agg).map(([region, amount]) => ({ region, amount }));
          arr.sort((a,b)=> b.amount - a.amount);
          return { arr, total };
        }
        function groupByDate(rows) {
          const agg = {};
          for (const r of rows || []) {
            const d = r.date_key; const v = Number(r.revenue_amount || 0);
            agg[d] = (agg[d] || 0) + v;
          }
          return Object.entries(agg).sort((a,b)=> new Date(a[0]) - new Date(b[0])).map(([d,v])=>({ d, v:Number(v) }));
        }
        function drawSparkline(svgId, series) {
          const svg = qs(svgId);
          if (!svg) return;
          svg.innerHTML = "";
          const n = series.length;
          if (n < 2) return;
          let min = Math.min(...series.map(p=>p.v));
          let max = Math.max(...series.map(p=>p.v));
          if (min === max) { min = 0; }
          const points = series.map((p,i)=>{
            const x = (i/(n-1))*100;
            const y = 30 - ((p.v - min)/(max - min)) * 28 - 1;
            return x.toFixed(2)+","+y.toFixed(2);
          }).join(" " );
          const poly = document.createElementNS("http://www.w3.org/2000/svg","polyline");
          poly.setAttribute("fill","none");
          poly.setAttribute("stroke","currentColor");
          poly.setAttribute("stroke-width","2");
          poly.setAttribute("points", points);
          svg.appendChild(poly);
        }
        async function getJSON(path) {
          const r = await fetch(path);
          if (!r.ok) throw new Error('Request failed: ' + r.status);
          return r.json();
        }
        async function loadAll() {
          const start = qs('startDate').value; const end = qs('endDate').value;

          try {
            const mrr = await getJSON('/metrics/mrr');
            setText('mrrValue', fmtMoney(mrr.mrr));
            setText('mrrMonth', 'Month ' + mrr.month);
          } catch (e) { setText('mrrValue', '—'); setText('mrrMonth', 'No data'); }

          try {
            const churn = await getJSON('/metrics/churn' + buildQuery({ day: end || undefined }));
            setText('churnRate', fmtPct(churn.churn_rate));
            setText('churnDetail', 'Cancels ' + churn.cancellations + ' • Prev Active ' + churn.prev_active + ' • Date ' + churn.date);
          } catch (e) { setText('churnRate', '—'); setText('churnDetail', 'No data'); }

          try {
            const rev = await getJSON('/metrics/revenue_by_region' + buildQuery({ start_date: start || undefined, end_date: end || undefined }));
            const rows = rev.rows || [];
            const recent = rows.slice(-10).map(r => [r.date_key, r.region_key, fmtMoney(r.revenue_amount)]);
            renderRows('rev-rows', recent, 3);
            const { arr, total } = groupByRegion(rows);
            setText('regionsTotal', 'Total ' + fmtMoney(total));
            if (arr.length === 0) { qs('regionsList').innerHTML = '<div class=\"muted\">No data</div>'; }
            else {
              const max = arr[0].amount || 1;
              qs('regionsList').innerHTML = arr.slice(0,5).map(({region, amount}) => {
                const pct = Math.max(2, Math.round((amount / max) * 100));
                return '<div style=\"margin:8px 0;\">'
                  + '<div style=\"display:flex; justify-content: space-between;\"><b>' + region + '</b><span class=\"muted\">' + fmtMoney(amount) + '</span></div>'
                  + '<div class=\"bar\"><b style=\"width:' + pct + '%\"></b></div>'
                  + '</div>';
              }).join('');
            }
            const byDate = groupByDate(rows);
            drawSparkline('revSpark', byDate);
          } catch (e) {
            renderRows('rev-rows', [], 3);
            qs('regionsList').innerHTML = '<div class=\"muted\">No data</div>';
            setText('regionsTotal', '—');
          }

          try {
            const fa = await getJSON('/metrics/forecast_vs_actual' + buildQuery({ start_date: start || undefined, end_date: end || undefined }));
            const rows = (fa.rows || []).slice(-10).map(r => {
              const variance = Number(r.variance || 0);
              const variancePct = Number(r.variance_pct || 0);
              const pillClass = variance >= 0 ? 'pill good' : 'pill bad';
              const pillText = (variance >= 0 ? '+' : '') + fmtMoney(variance) + ' (' + (variancePct>=0?'+':'') + (variancePct*100).toFixed(2) + '%)';
              return [r.date, fmtMoney(r.actual), fmtMoney(r.forecast), '<span class=\"' + pillClass + '\">' + pillText + '</span>', (variancePct*100).toFixed(2) + '%'];
            });
            renderRows('fa-rows', rows, 5);
          } catch (e) { renderRows('fa-rows', [], 5); }

          setText('updatedAt', new Date().toLocaleString());
        }
        qs('apply').addEventListener('click', loadAll);
        qs('refresh').addEventListener('click', () => { qs('startDate').value=''; qs('endDate').value=''; loadAll(); });
        document.documentElement.setAttribute('data-theme', localStorage.getItem('theme') || 'light');
        const themeBtn = qs('themeToggle');
        if (themeBtn) themeBtn.addEventListener('click', () => {
          const cur = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
          document.documentElement.setAttribute('data-theme', cur);
          localStorage.setItem('theme', cur);
        });
        loadAll();
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
