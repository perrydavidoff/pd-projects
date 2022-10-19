-- author: Perry Davidoff
-- team: etsy ads / sadx
-- overview: this table was created to power analyses into the relationship between budget constrained budget and revenue.
-- bc 3mo, 6mo, + 1 year was used because budget constrained budget increases could lead to sellers no longer being BC.
-- date: march, 2022

-- create base tables to calculate impression gaps for model
create or replace table
   `etsy-data-warehouse-dev.pdavidoff.impression_gap_historical`
   as ( 
select
	distinct
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "listing_id") as prolist_listings
from
	`etsy-visit-pipe-prod.canonical.visits_sampled` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_ranking_signals"
where
	date(datetime(timestamp_millis(b.epoch_ms),"America/New_York")) >= "2018-01-01"
)
;

-- impression gap changes
create or replace table
   `etsy-data-warehouse-dev.pdavidoff.impression_gap_agg`
   as ( 
select
	-- distinct
	date(event_time) as date,
	count(case when prolist_listings = "[]" then visit_id end)/count(visit_id) as impression_gap,
	count(case when extract(hour from event_time) between 0 and 5 then visit_id end) as em_requests,
	count(case when extract(hour from event_time) between 18 and 23 then visit_id end) as eve_requests,
	count(case when extract(hour from event_time) between 0 and 5 and prolist_listings = "[]" then visit_id end)/count(case when extract(hour from event_time) between 0 and 5 then visit_id end) as em_impression_gap,
	count(case when extract(hour from event_time) between 18 and 23 and prolist_listings = "[]" then visit_id end)/count(case when extract(hour from event_time) between 18 and 23 then visit_id end) as eve_impression_gap
from
	`etsy-data-warehouse-dev.pdavidoff.impression_gap_historical`
group by 1
order by 1
)
;

-- final table with the inputs from the different base tables
  `etsy-data-warehouse-dev.pdavidoff.bc_budget_and_revenue`
  as (
with base as (
  	select
  		date,
  		shop_id,
  		budget_constrained_shop,
  		max(budget_constrained_shop) over(partition by shop_id order by date rows between 90 preceding and current row) as bc_3mo,
  		max(budget_constrained_shop) over(partition by shop_id order by date rows between 180 preceding and current row) as bc_6mo,
  		max(budget_constrained_shop) over(partition by shop_id order by date rows between 365 preceding and current row) as bc_1_yr
  	from
  		`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
  	where
  		date >= "2016-01-01"
  	group by 1,2,3
),base2 as (
select
	a.date,
	count(distinct case when b.budget_constrained_shop = 1 then a.shop_id end) as bc_shops,
	count(distinct case when bc_3mo = 1 then a.shop_id end) as bc_3mo_shops,
	count(distinct case when bc_6mo = 1 then a.shop_id end) as bc_6mo_shops,
	count(distinct case when bc_1_yr = 1 then a.shop_id end) as bc_1yr_shops,
	count(distinct case when b.budget_constrained_shop != 1 then a.shop_id end) as non_bc_shops,
	count(distinct case when bc_3mo != 1 then a.shop_id end) as non_bc_3mo_shops,
	count(distinct case when bc_6mo != 1 then a.shop_id end) as non_bc_6mo_shops,
	count(distinct case when bc_1_yr != 1 then a.shop_id end) as non_bc_1yr_shops,
	sum(case when b.budget_constrained_shop = 1 then budget end) as bc_budget,
	sum(case when bc_3mo = 1 then budget end) as bc_3mo_budget,
	sum(case when bc_6mo = 1 then budget end) as bc_6mo_budget,
	sum(case when bc_1_yr = 1 then budget end) as bc_1yr_budget,
	sum(case when b.budget_constrained_shop = 1 then spend end) as bc_spend,
	sum(case when bc_3mo = 1 then spend end) as bc_3mo_spend,
	sum(case when bc_6mo = 1 then spend end) as bc_6mo_spend,
	sum(case when bc_1_yr = 1 then spend end) as bc_1yr_spend,
	sum(case when b.budget_constrained_shop != 1 then budget end) as non_bc_budget,
	sum(case when bc_3mo != 1 then budget end) as non_bc_3mo_budget,
	sum(case when bc_6mo != 1 then budget end) as non_bc_6mo_budget,
	sum(case when bc_1_yr != 1 then budget end) as non_bc_1yr_budget,
	sum(case when b.budget_constrained_shop != 1 then spend end) as non_bc_spend,
	sum(case when bc_3mo != 1 then spend end) as non_bc_3mo_spend,
	sum(case when bc_6mo != 1 then spend end) as non_bc_6mo_spend,
	sum(case when bc_1_yr != 1 then spend end) as non_bc_1yr_spend
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a left join 
	base b on a.shop_id = b.shop_id and a.date = b.date
where
	a.date >= "2017-01-01"
group by 1
)
select
	a.*,
	impressions as total_impressions,
	clicks/impressions as ctr,
	etsy_visits as etsy_visits,
	impressions/etsy_visits as impressions_per_visit,
	budget as total_budget,
	spend as total_spend,
	impression_gap,
	spend/clicks as cost_per_click,
	converting_clicks as converting_clicks,
	converting_clicks/clicks as pccr,
	em_impression_gap,
	eve_impression_gap
from
	base2 a left join 
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary` b on a.date = b.date left join 
	`etsy-data-warehouse-dev.pdavidoff.impression_gap_agg` c on a.date = c.date
)
;






-- with budget_constrained_base as (
-- select
-- 	shop_id,
-- 	date_trunc(date,week) as week,
-- 	max(budget_constrained_shop) as budget_constrained_shop
-- 	-- min(case when budget_constrained_shop = 1 then date_trunc(date,week) end) as bc_seller_week
-- from
-- 	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
-- where
-- 	date >= "2019-01-01" and budget_constrained_shop = 1
-- group by 1,2
-- ),shop_data as (
-- select
-- 	distinct
-- 	shop_id,
-- 	date,
-- 	budget,
-- 	spend
-- from
-- 	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
-- 	)
-- ),cross_join as (
-- select
	
-- from
-- 	)
-- select
-- 	date_trunc(date,week) as week,
-- 	count(case when bc_seller_week >= date_trunc(date,week) then a.shop_id end) as bc_sellers,
-- 	count(case when bc_seller_week is null or bc_seller_week < date_trunc(date,week) then a.shop_id end) as non_bc_sellers,
-- 	sum(case when bc_seller_week >= date_trunc(date,week) then budget end) as bc_budget,
-- 	sum(case when bc_seller_week is null or bc_seller_week < date_trunc(date,week) then budget end) as non_bc_budget,
-- 	sum(case when bc_seller_week >= date_trunc(date,week) then budget end)/sum(budget) as bc_budget_share,
-- 	sum(case when bc_seller_week >= date_trunc(date,week) then spend end) as bc_spend,
-- 	sum(case when bc_seller_week is null or bc_seller_week < date_trunc(date,week) then spend end) as non_bc_spend,
-- 	sum(case when bc_seller_week >= date_trunc(date,week) then spend end)/sum(spend) as bc_spend_share,
-- 	sum(case when bc_seller_week >= date_trunc(date,week) then impression_count end) as bc_imp_count,
-- 	sum(case when bc_seller_week is null or bc_seller_week < date_trunc(date,week) then impression_count end) as non_bc_imp_count,
-- 	sum(case when bc_seller_week >= date_trunc(date,week) then impression_count end)/sum(impression_count) as bc_imp_count_share
-- from
-- 	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a left join
-- 	budget_constrained_base b on a.shop_id = b.shop_id and a.
-- where
-- 	date between "2021-06-27" and "2021-10-01"
-- group by 1
-- order by 1
-- ;

-- select
-- 	date_trunc(date,week) as week,
-- 	sum(case when budget_constrained_shop = 1 then budget end) as bc_budget,
-- 	sum(case when budget_constrained_shop = 0 then budget end) as non_bc_budget,
-- 	sum(case when budget_constrained_shop = 1 then budget end)/sum(budget) as bc_budget_share,
-- 	sum(case when budget_constrained_shop = 1 then spend end) as bc_spend,
-- 	sum(case when budget_constrained_shop = 0 then spend end) as non_bc_spend,
-- 	sum(case when budget_constrained_shop = 1 then spend end)/sum(spend) as bc_spend_share,
-- 	sum(case when budget_constrained_shop = 1 then imp_count end) as bc_imp_count,
-- 	sum(case when budget_constrained_shop = 0 then imp_count end) as non_bc_imp_count,
-- 	sum(case when budget_constrained_shop = 1 then imp_count end)/sum(imp_count) as bc_imp_count_share
-- from
-- 	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
-- where
-- 	date >= "2018-01-01"
-- group by 1
-- order by 1
-- ;



-- with base as (
-- select
-- 	date(datetime(event_time, "America/New_York")) as event_date_est,
-- 	extract(hour from datetime(event_time, "America/New_York")) as event_hour_est,
-- 	prolist_listings,
-- 	visit_id
-- from
-- 	`etsy-data-warehouse-dev.pdavidoff.impression_gap_historical`
-- )
-- select
-- 	date_trunc(event_date_est,week) as event_week_est,
-- 	count(case when prolist_listings = "[]" then visit_id end)/count(visit_id) as overall_impression_gap,
-- 	count(case when event_hour_est between 0 and 5 then visit_id end) as em_requests,
-- 	count(case when event_hour_est between 18 and 23 then visit_id end) as eve_requests,
-- 	count(case when event_hour_est between 0 and 5 and prolist_listings = "[]" then visit_id end)/count(case when event_hour_est between 0 and 5 then visit_id end) as em_impression_gap,
-- 	count(case when event_hour_est between 18 and 23 and prolist_listings = "[]" then visit_id end)/count(case when event_hour_est between 18 and 23 then visit_id end) as eve_impression_gap
-- from
-- 	base
-- group by 1
-- order by 1
-- ;
-- 	-- relevance gap table
-- -- clicks from attributed impression table
-- create or replace table
--    `etsy-data-warehouse-dev.pdavidoff.relevance_gap_historical`
--    as ( 
-- with base as (
-- select
-- 	distinct
-- 	visit_id,
-- 	logging_key,
-- 	click,
-- 	timestamp_seconds(cast(timestamp as int64)) as 
-- from
-- 	`etsy-prolist-etl-prod.prolist.attributed_impressions`
-- where
-- 	timestamp_seconds(cast(timestamp as int64)) between "2021-07-01" and "2021-10-01"
-- )
-- ;