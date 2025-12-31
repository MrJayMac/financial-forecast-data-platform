create or replace view fact_usage_daily as
select
  event_date as date_key,
  region as region_key,
  metric_name,
  sum(units) as total_units,
  count(*) as usage_events
from stg_usage_events
group by 1,2,3;
