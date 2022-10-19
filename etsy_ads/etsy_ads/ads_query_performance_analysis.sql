

select
	event_date_est,
	count(distinct query) as query_count,
	sum(total_ads_queries) as total_ads_queries,
	sum(total_ads_empty_queries)/sum(total_ads_queries) as empty_query_share,
	sum(total_ads_impressions) as total_ads_impressions,
	sum(total_ads_clicks)/sum(total_ads_impressions) as ads_ctr,
	sum(total_ads_cost)/sum(total_ads_clicks) as ads_cpc,
	sum(total_ads_orders)/sum(total_ads_clicks) as ads_pccr,
	sum(total_ads_gms)/sum(total_ads_orders) as ads_gms_per_attrib_order,
	sum(ads_budget_imp)/sum(total_ads_impressions) as budget_per_imp,
	sum(ads_spend_imp)/sum(total_ads_impressions) as spend_per_imp,
	sum(ads_spend_imp)/sum(ads_budget_imp) as budget_util_per_imp,
	sum(cs_listing_ads_imp)/sum(total_ads_impressions) as cs_listing_imp_share,
	sum(cs_seller_ads_imp)/sum(total_ads_impressions) as cs_seller_imp_share,
	sum(new_listing_7d_ads_imp)/sum(total_ads_impressions) as new_listing_imp_share,
	sum(top_shop_ads_imp)/sum(total_ads_impressions) as top_shop_imp_share
from
	`etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance`
group by 1
order by 1
;

-- by page type
select
	page_type_group,
	count(distinct query) as query_count,
	sum(total_ads_queries) as total_ads_queries,
	sum(total_ads_empty_queries)/sum(total_ads_queries) as empty_query_share,
	sum(total_ads_impressions) as total_ads_impressions,
	sum(total_ads_clicks)/sum(total_ads_impressions) as ads_ctr,
	sum(total_ads_cost)/sum(total_ads_clicks) as ads_cpc,
	sum(total_ads_orders)/sum(total_ads_clicks) as ads_pccr,
	sum(total_ads_gms)/sum(total_ads_orders) as ads_gms_per_attrib_order,
	sum(ads_budget_imp)/sum(total_ads_impressions) as budget_per_imp,
	sum(ads_spend_imp)/sum(total_ads_impressions) as spend_per_imp,
	sum(ads_spend_imp)/sum(ads_budget_imp) as budget_util_per_imp,
	sum(cs_listing_ads_imp)/sum(total_ads_impressions) as cs_listing_imp_share,
	sum(cs_seller_ads_imp)/sum(total_ads_impressions) as cs_seller_imp_share,
	sum(new_listing_7d_ads_imp)/sum(total_ads_impressions) as new_listing_imp_share,
	sum(top_shop_ads_imp)/sum(total_ads_impressions) as top_shop_imp_share
from
	`etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance`
where
	event_date_est = "2022-02-05"
group by 1
order by 5 desc
;

-- top 1000 queries
with base as (
select
	query,
	query_date_rank,
	query_top_category,
	query_group_label,
	-- count(distinct query) as query_count,
	sum(total_ads_queries) as total_ads_queries,
	safe_divide(sum(total_ads_empty_queries),sum(total_ads_queries)) as empty_query_share,
	sum(total_ads_impressions) as total_ads_impressions,
	safe_divide(sum(total_ads_clicks),sum(total_ads_impressions)) as ads_ctr,
	safe_divide(sum(total_ads_cost),sum(total_ads_clicks)) as ads_cpc,
	safe_divide(sum(total_ads_orders),sum(total_ads_clicks)) as ads_pccr,
	safe_divide(sum(total_ads_gms),sum(total_ads_orders)) as ads_gms_per_attrib_order,
	safe_divide(sum(ads_budget_imp),sum(total_ads_impressions)) as budget_per_imp,
	safe_divide(sum(ads_spend_imp),sum(total_ads_impressions)) as spend_per_imp,
	safe_divide(sum(ads_spend_imp),sum(ads_budget_imp)) as budget_util_per_imp,
	safe_divide(sum(cs_listing_ads_imp),sum(total_ads_impressions)) as cs_listing_imp_share,
	safe_divide(sum(cs_seller_ads_imp),sum(total_ads_impressions)) as cs_seller_imp_share,
	safe_divide(sum(new_listing_7d_ads_imp),sum(total_ads_impressions)) as new_listing_imp_share,
	safe_divide(sum(top_shop_ads_imp),sum(total_ads_impressions)) as top_shop_imp_share,
	sum(total_search_impressions) as total_search_imps,
	-- relevance
	safe_divide(sum(em_ads_clicks),sum(em_ads_impressions)) as em_ctr,
	safe_divide(sum(lm_ads_clicks),sum(lm_ads_impressions)) as lm_ctr,
	safe_divide(sum(an_ads_clicks),sum(an_ads_impressions)) as an_ctr,
	safe_divide(sum(eve_ads_clicks),sum(eve_ads_impressions)) as eve_ctr,
	-- -- search
	safe_divide(sum(em_search_clicks),sum(em_search_impressions)) as em_org_ctr,
	safe_divide(sum(lm_search_clicks),sum(lm_search_impressions)) as lm_org_ctr,
	safe_divide(sum(an_search_clicks),sum(an_search_impressions)) as an_org_ctr,
	safe_divide(sum(eve_search_clicks),sum(eve_search_impressions)) as eve_org_ctr,
	-- impression gap
	safe_divide(sum(em_ads_empty_queries),sum(em_ads_queries)) as em_impression_gap,
	safe_divide(sum(lm_ads_empty_queries),sum(lm_ads_queries)) as lm_impression_gap,
	safe_divide(sum(an_ads_empty_queries),sum(an_ads_queries)) as an_impression_gap,
	safe_divide(sum(eve_ads_empty_queries),sum(eve_ads_queries)) as eve_impression_gap
from
	`etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance`	
where
	event_date_est = "2022-02-05" and page_type_group = "Search" and query_date_rank <= 100
group by 1,2,3,4
order by query_date_rank
;

-- look into turning query raw into query
-- use raw query from the query sessions table to get the data.
-- seems like there are a lot of raw queries in the rollup that aren't in the query sessions new table.
-- there's only 62%-64% coverage of search queries in the query sessions new table. weird.
-- query i used below.
-- check against the existing queries
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.stemmed_query_check`
  as (
with join_query_table as (
select
	distinct
	a.query as query_raw,
	b.query as stemmed_query
from
	`etsy-data-warehouse-prod.rollups.etsy_ads_daily_query_performance` a left join
	`etsy-data-warehouse-prod.search.query_sessions_new` b on trim(lower(a.query)) = trim(lower(b.query_raw)) and b._date >= current_date - 30
-- where
-- 	event_date_est in ("2022-02-05","2022-02-06")
)
select
	a.*,
	b.stemmed_query	
from
	`etsy-data-warehouse-prod.rollups.etsy_ads_daily_query_performance` a left join
	join_query_table b on a.query = b.query_raw
-- where
-- 	event_date_est in ("2022-02-05","2022-02-06")
)
;

-- let's try to reset the tables and start from scratch.
-- create tables that pull the raw query events and prolist impression events.

-- raw ads query events
-- first, pull all of the raw prolist ranking signal events.
create or replace table
`etsy-data-warehouse-dev.pdavidoff.raw_query_events`
  as (
select
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "page_type") as page_type,
	(select value from unnest(properties.map) where key = "listing_id") as prolist_listings
from
	`etsy-visit-pipe-prod.canonical.visits_recent` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_ranking_signals"
where
	date(datetime(_PARTITIONTIME,"America/New_York")) in ("2022-02-05","2022-02-06","2022-02-07")
)
;

-- raw ads query impressions
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.raw_prolist_imps`
  as (
select
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "listing_id") as listing_id,	
	(select value from unnest(properties.map) where key = "nonce") as nonce,
	(select value from unnest(properties.map) where key = "logging_key") as logging_key,	
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "page_type") as page_type
from
	`etsy-visit-pipe-prod.canonical.visits_recent` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_imp_full"
where
	date(datetime(_PARTITIONTIME,"America/New_York")) in ("2022-02-05","2022-02-06","2022-02-07")
)
;


-- pull the raw search events
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.search_events`
  as (
select
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "query") as raw_query,	
	(select value from unnest(properties.map) where key = "processed_query") as processed_query,
	(select value from unnest(properties.map) where key = "page") as page,	
	(select value from unnest(properties.map) where key = "total_results") as total_results
from
	`etsy-visit-pipe-prod.canonical.visits_recent` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "search"
where
	date(datetime(_PARTITIONTIME,"America/New_York")) in ("2022-02-05","2022-02-06","2022-02-07")
)
;

-- ok, first look at how query sessions and imps come together
select
	max(event_date_est)
from
	`etsy-data-warehouse-prod.rollups.etsy_ads_hourly_performance`
;

-- overall metrics



select
	event_date_est,
	count(case when stemmed_query is not null then query end)/count(query) as stemmed_query_coverage
from
	`etsy-data-warehouse-dev.pdavidoff.stemmed_query_check`
where
	page_type_group = "Search"
group by 1
order by 1
;

-- check against raw search events
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.raw_search_event_compare`
  as (
with join_query_table as (
select
	distinct
	a.query as query_raw,
	b.processed_query as processed_query
from
	`etsy-data-warehouse-prod.rollups.etsy_ads_daily_query_performance` a left join
	`etsy-data-warehouse-dev.pdavidoff.search_events` b on trim(lower(a.query)) = trim(lower(b.raw_query))
where
	event_date_est in ("2022-02-05","2022-02-06")
)
select
	a.*,
	b.processed_query	
from
	`etsy-data-warehouse-prod.rollups.etsy_ads_daily_query_performance` a left join
	join_query_table b on lower(trim(a.query)) = lower(trim(b.query_raw))
where
	event_date_est in ("2022-02-05","2022-02-06")
)
;

create or replace table
  `etsy-data-warehouse-dev.pdavidoff.raw_events_compare`
  as (
with search_base as (
select
	distinct
	raw_query as query
from
	`etsy-data-warehouse-dev.pdavidoff.search_events`
),base2 as (
select
	distinct
	query
from
	`etsy-data-warehouse-dev.pdavidoff.raw_prolist_imps`
where
	page_type = "0"
),base3 as (
select
	a.query as prolist_query,
	b.query as search_query
from
	base2 a full outer join
	search_base b on lower(trim(a.query)) = lower(trim(b.query))
)
select
	a.*,
	b.query as stemmed_query
from
	base3 a left join
	`etsy-data-warehouse-prod.search.events` b on lower(trim(a.prolist_query)) = lower(trim(b.query_raw)) and date(datetime(b.epoch_ms,"America/New_York")) in ("2022-02-05","2022-02-06") and b._date >= "2022-02-04"
)
;


select
	max(_date)
from
	`etsy-data-warehouse-prod.search.events`
where
	_date >= "2022-02-01"
;

select
	count(*)
from
	`etsy-data-warehouse-prod.search.events`
where
	date(datetime(epoch_ms,"America/New_York")) in ("2022-02-05","2022-02-06")
	and _date >= "2022-02-04"
;
select
	query,
	processed_query,
	page_type_group,
	total_ads_impressions
from
	`etsy-data-warehouse-dev.pdavidoff.raw_search_event_compare`
order by total_ads_impressions desc
limit 50;

select
	count(distinct case when processed_query is not null then query end)/count(distinct query) as coverage
from
	`etsy-data-warehouse-dev.pdavidoff.raw_search_event_compare`
where
	page_type_group = "Search"
;

select
	sum(total_search_visits_with_imp) as search_visits_with_imps,
	sum(total_ads_visits_with_imp) as ads_visits_with_imps
from
	`etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance`
where
	event_date_est = "2022-02-05" and page_type_group = "Search"
;


select
	query,
	stemmed_query,
	page_type_group,
	total_ads_queries
from
	`etsy-data-warehouse-dev.pdavidoff.stemmed_query_check`
where
	event_date_est = "2022-02-05" and page_type_group = "Search" and stemmed_query is null
order by total_ads_queries desc
limit 50;


select
	page_type_group,
	count(case when stemmed_query is null then query end)/count(query) as query_share
from
	`etsy-data-warehouse-dev.pdavidoff.stemmed_query_check`
where
	page_type_group != "Category"
group by 1
;

select
	query_raw,
	query,
	count(*) as query_count
from
	`etsy-data-warehouse-prod.search.events`
where
	_date = "2022-02-05" and page = 1
group by 1,2
order by 3 desc
limit 50
;

select
	query,
	sum(total_ads_queries) as ads_queries
from
	`etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance`
where
	page_type_group = "Search" and event_date_est = "2022-02-05"
group by 1
order by 2 desc
;



select
	query,
	stemmed_query
from
	`etsy-data-warehouse-dev.pdavidoff.stemmed_query_check`
where
	stemmed_query is null
limit 50;
select
	distinct
	date(datetime(_PARTITIONTIME,"America/New_York")) as date,
	date(datetime(_PARTITIONTIME,"America/New_York")) = current_date - 2
from
	`etsy-visit-pipe-prod.canonical.visits_recent`
where
	_PARTITIONTIME >= "2022-02-04"
;