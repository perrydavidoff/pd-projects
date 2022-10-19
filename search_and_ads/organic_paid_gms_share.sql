create or replace table
  `etsy-data-warehouse-dev.pdavidoff.organic_paid_gms`
  as (
with search_gms as (
select
	date_trunc(_date,month) as month,
	visit_id,
	sum(attributed_gms) as search_attr_gms	
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date >= "2019-01-01" and has_purchase = 1
group by 1,2
),ads_gms as (
select
	date_trunc(click_timestamp,month) as month,
	visit_id,
	-- page,
	sum(gms) as etsy_ads_gms
from
	`etsy-data-warehouse-prod.rollups.prolist_click_mart`
where
	click_timestamp >= "2019-01-01" and page in ("search","category","market")
	and gms > 0
group by 1,2
)
select
	a.month,
	case when a.visit_id is not null then a.visit_id else b.visit_id end as visit_id,
  	max(case 
  		when a.visit_id is not null and b.visit_id is not null then "both"
  		when a.visit_id is not null and b.visit_id is null then "search"
  		when a.visit_id is null and b.visit_id is not null then "etsy ads"
  		else "other" end) as visit_cat,
	max(case when a.visit_id is not null then search_attr_gms else etsy_ads_gms end) as attr_gms
from
	search_gms a full outer join
	ads_gms b on a.visit_id = b.visit_id
group by 1,2
)
;
select
	referring_page_event,
	count(*)
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date >= "2022-03-06" and platform in ("desktop","mobile_web")
group by 1
order by 2 desc
;

select
	ref_tag,
	count(*)
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date >= "2022-03-06" and referring_page_event = "category_page" and platform in ("desktop","mobile_web")
group by 1
order by 2 desc
;

-- listing view break out
select
	date_trunc(_date,month) as month,
	count(*) as listing_views,
	count(case when ref_tag like "%sc_gallery%" or ref_tag like "%listing_page_ad_row%" then listing_id end) as ea_listing_views,
	count(case when ref_tag like "%sr_gallery%" then listing_id end) as organic_listing_views,
	count(case when purchased_after_view = 1 then listing_id end) as purchases,
	count(case when ref_tag like "%sc_gallery%" or ref_tag like "%listing_page_ad_row%" and purchased_after_view = 1 then listing_id end) as ea_listing_view_purchase,
	count(case when ref_tag like "%sr_gallery%" or ref_tag like "%listing_page_ad_row%" and purchased_after_view = 1 then listing_id end) as organic_listing_view_purchase,
	sum(case when purchased_after_view = 1 then price_usd end) as overall_gms,
	sum(case when ref_tag like "%sc_gallery%" or ref_tag like "%listing_page_ad_row%" and purchased_after_view = 1 then price_usd end) as ea_listing_view_gms,
	sum(case when ref_tag like "%sr_gallery%" and purchased_after_view = 1 then price_usd end) as organic_listing_view_gms
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date >= "2019-01-01" and platform in ("desktop","mobile_web")
group by 1
order by 1
;

select
	count(*)
from
	`etsy-data-warehouse-prod.weblog.events` a join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b.platform in ("desktop","mobile_web") and b._date >= "2022-03-01"
where
	event_type = "prolist_click_full" and a._date >= "2022-03-01"
;

with base as (
select
	date_trunc(_date,month) as month,
	visit_id,
	sum(trans_gms_gross) as total_gms
from
	`etsy-data-warehouse-prod.visit_mart.visits_transactions`
where
	_date >= "2019-01-01"
group by 1,2
)
select
	a.month,
	count(distinct a.visit_id) as visit_w_conversion,
	count(distinct b.visit_id) as search_and_ads_converting_visits,
	sum(total_gms) as overall_gms,
	sum(case when visit_cat = "both" then total_gms end) as both_gms,
	sum(case when visit_cat = "search" then total_gms end) as search_gms,
	sum(case when visit_cat = "etsy ads" then total_gms end) as etsy_ads_gms
from
	base a left join
	`etsy-data-warehouse-dev.pdavidoff.organic_paid_gms` b on a.visit_id = b.visit_id
group by 1
order by 1
;

-- listing view share



-- create or replace table
--   `etsy-data-warehouse-dev.pdavidoff.organic_paid_gms`
--   as (
-- with search_gms as (
-- select
-- 	date_trunc(_date,month) as month,
-- 	visit_id,
-- 	sum(attributed_gms) as search_attr_gms	
-- from
-- 	`etsy-data-warehouse-prod.search.query_sessions_new`
-- where
-- 	_date >= "2021-01-01" and has_purchase = 1
-- group by 1,2
-- ),ads_gms as (
-- select
-- 	date_trunc(click_timestamp,month) as month,
-- 	visit_id,
-- 	-- page,
-- 	sum(gms) as etsy_ads_gms
-- from
-- 	`etsy-data-warehouse-prod.rollups.prolist_click_mart`
-- where
-- 	click_timestamp >= "2021-01-01" and page in ("search","category","market")
-- 	and gms > 0
-- group by 1,2
-- )
-- select
-- 	a.month,
-- 	case when a.visit_id is not null then search_visit else paid_visit end) as visit_id,
--   	max(case when a.visit_id is not null then "search" else "paid" end) as visit_cat,
-- 	max(case when a.visit_id is not null then search_attr_gms else etsy_ads_gms end) as attr_gms
-- from
-- 	search_gms a full outer join
-- 	ads_gms b on a.visit_id = b.visit_id
-- group by 1,2
-- )
-- ;



