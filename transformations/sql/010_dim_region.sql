create or replace view dim_region as
select distinct region as region_key
from events_raw
where region is not null and region <> '';
