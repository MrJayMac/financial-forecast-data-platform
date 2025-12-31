create or replace view stg_usage_events as
select
  event_id,
  (payload::jsonb ->> 'metric_name') as metric_name,
  (payload::jsonb ->> 'units')::int as units,
  (payload::jsonb ->> 'plan_id') as plan_id,
  customer_id,
  region,
  date_trunc('day', event_time)::date as event_date,
  event_time,
  payload
from events_raw
where event_type = 'usage';
