create or replace view fact_costs_daily as
select
  event_date as date_key,
  region as region_key,
  sum(amount) as cost_amount,
  count(*) as costs_count
from stg_cost_events
group by 1,2;
