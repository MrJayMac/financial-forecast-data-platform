create or replace view stg_cost_events as
select
  event_id,
  (payload::jsonb ->> 'amount')::numeric as amount,
  (payload::jsonb ->> 'cost_type') as cost_type,
  customer_id,
  region,
  date_trunc('day', event_time)::date as event_date,
  event_time,
  payload
from events_raw
where event_type = 'cost';
