-- Active subscriptions snapshot per day
create or replace view fact_subscriptions_snapshot as
with changes as (
  select event_date::date as date_key,
         customer_id,
         (payload::jsonb ->> 'plan_id') as plan_id,
         case when (payload::jsonb ->> 'action') = 'created' then 1
              when (payload::jsonb ->> 'action') = 'canceled' then -1
              else 0 end as delta
  from stg_subscription_events
),
net_changes as (
  select date_key, sum(delta) as net_delta
  from changes
  group by 1
),
seq as (
  select d.date_key
  from dim_time d
),
agg as (
  select s.date_key,
         sum(n.net_delta) over (order by s.date_key rows between unbounded preceding and current row) as active_subscriptions
  from seq s
  left join net_changes n on n.date_key = s.date_key
)
select date_key, coalesce(active_subscriptions, 0) as active_subscriptions
from agg;
