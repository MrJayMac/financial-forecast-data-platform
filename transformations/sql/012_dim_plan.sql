create or replace view dim_plan as
with plans as (
  select (payload::jsonb ->> 'plan_id') as plan_id from stg_subscription_events where (payload::jsonb ->> 'plan_id') is not null
  union
  select (payload::jsonb ->> 'plan_id') as plan_id from stg_usage_events where (payload::jsonb ->> 'plan_id') is not null
)
select plan_id as plan_key
from plans
where plan_id is not null and plan_id <> '';
