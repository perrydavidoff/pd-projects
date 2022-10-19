create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.catapult_prolist_clicks`
	as (
select
	distinct
	_date as date,
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "cost") as total_cost
from
	`etsy-visit-pipe-prod.canonical.visits` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_click_full"
where
	_date BETWEEN "2022-04-01" and "2022-04-27"
)
;

with base as (
select
	
from
	)