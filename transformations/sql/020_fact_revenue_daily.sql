create or replace view fact_revenue_daily as
select
  event_date as date_key,
  region as region_key,
  sum(amount) as revenue_amount,
  count(*) as payments_count
from stg_payment_events
group by 1,2;
