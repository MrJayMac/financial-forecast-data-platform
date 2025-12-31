create or replace view stg_subscription_events as
select
  event_id,
  (payload::jsonb ->> 'action') as action,
  (payload::jsonb ->> 'plan_id') as plan_id,
  customer_id,
  region,
  date_trunc('day', event_time)::date as event_date,
  event_time,
  payload
from events_raw
where event_type = 'subscription';
