create or replace view stg_payment_events as
select
  event_id,
  (payload::jsonb ->> 'amount')::numeric as amount,
  (payload::jsonb ->> 'currency') as currency,
  (payload::jsonb ->> 'payment_method') as payment_method,
  customer_id,
  region,
  date_trunc('day', event_time)::date as event_date,
  event_time,
  payload
from events_raw
where event_type = 'payment';
