create or replace view dim_time as
with bounds as (
  select coalesce(min(date_trunc('day', event_time))::date, current_date) as start_date,
         coalesce(max(date_trunc('day', event_time))::date, current_date) as end_date
  from events_raw
), series as (
  select generate_series(start_date, greatest(end_date, current_date), interval '1 day')::date as date_key
  from bounds
)
select date_key,
       extract(year from date_key)::int as year,
       extract(quarter from date_key)::int as quarter,
       extract(month from date_key)::int as month,
       extract(day from date_key)::int as day
from series;
