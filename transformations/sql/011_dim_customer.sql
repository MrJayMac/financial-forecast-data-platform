create or replace view dim_customer as
select distinct on (customer_id)
  customer_id as customer_key,
  region as default_region,
  min(event_time) over (partition by customer_id) as first_seen_at
from events_raw
where customer_id is not null and customer_id <> '';
