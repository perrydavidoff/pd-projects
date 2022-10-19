-- owner: pdavidoff@etsy.com
-- owner_team: sa-pe-analytics@etsy.com
-- goal: Create a rollup that allows us to measure impression + relevance gaps along with cold start imp share.
-- see if the query level is feasible for this rollup, but it may be too big to be useful
-- update the table to EST since the ads model refreshes daily according to EST.


-- for page type numbers, see here: https://console.cloud.google.com/datacatalog/projects/etsy-prolist-etl-prod/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvZXRzeS1wcm9saXN0LWV0bC1wcm9kL2RhdGFzZXRzL3Byb2xpc3QvdGFibGVzL2F0dHJpYnV0ZWRfaW1wcmVzc2lvbnM?project=etsy-data-warehouse-prod

-- set macro date variable here: current_date - 2
BEGIN

DECLARE active_date STRING default "current_date - 2";

10 per ads chunk
12 listing char
4 per search chunk
8 search char

CREATE OR REPLACE TABLE
  `etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance`
  (
  	event_date_est DATE NOT NULL,
  	query STRING,
  	page_type_group STRING,
  	-- overall ads metrics
  	total_ads_queries INT64,
  	total_ads_empty_queries INT64,
  	total_ads_result_count INT64,
  	total_ads_visits_with_imp INT64,
  	total_ads_impressions INT64,
  	total_ads_clicks INT64,
  	total_ads_cost FLOAT64,
  	total_ads_orders INT64,
  	total_ads_gms FLOAT64,
  	-- early morning ads metrics
		em_ads_queries INT64,
		em_ads_empty_queries INT64,
		em_ads_result_count INT64,
		em_ads_visits_with_imp INT64,
		em_ads_impressions INT64,
		em_ads_clicks INT64,
		em_ads_cost FLOAT64,
		em_ads_purchases_1d FLOAT64,
		em_ads_gms_1d FLOAT64,
		-- late morning ads metrics
		lm_ads_queries INT64,
		lm_ads_empty_queries INT64,
		lm_ads_result_count INT64,
		lm_ads_visits_with_imp INT64,
		lm_ads_impressions INT64,
		lm_ads_clicks INT64,
		lm_ads_cost FLOAT64,
		lm_ads_purchases_1d FLOAT64,
		lm_ads_gms_1d FLOAT64,
		-- afternoon ads metrics
		an_ads_queries INT64,
		an_ads_empty_queries INT64,
		an_ads_result_count INT64,
		an_ads_visits_with_imp INT64,
		an_ads_impressions INT64,
		an_ads_clicks INT64,
		an_ads_cost FLOAT64,
		an_ads_purchases_1d FLOAT64,
		an_ads_gms_1d FLOAT64,
		-- eve ads metrics
		eve_ads_queries INT64,
		eve_ads_empty_queries INT64,
		eve_ads_result_count INT64,
		eve_ads_visits_with_imp INT64,
		eve_ads_impressions INT64,
		eve_ads_clicks INT64,
		eve_ads_cost FLOAT64,
		eve_ads_purchases_1d FLOAT64,
		eve_ads_gms_1d FLOAT64,
		-- ads listing characteristics
		distinct_listing_ads_imp INT64,
		distinct_shops_ads_imp INT64,
		ads_budget_imp FLOAT64,
		ads_spend_imp FLOAT64,
		budget_constrained_shop_ads_imp INT64,
		cs_listing_ads_imp INT64,
		cs_seller_ads_imp INT64,
		new_listing_7d_ads_imp INT64,
		new_listing_30d_ads_imp INT64,
		top_shop_ads_imp INT64,
		power_shop_ads_imp INT64,
  	-- overall search metrics
		total_search_visits_with_imp INT64,
		total_search_impressions INT64,
		total_search_clicks INT64,
		total_search_purchases INT64,
		-- early morning search metrics
		em_search_visits_with_imp INT64,
		em_search_impressions INT64,
		em_search_clicks INT64,
		em_search_purchases_1d INT64,
		-- late morning search metrics
		lm_search_visits_with_imp INT64,
		lm_search_impressions INT64,
		lm_search_clicks INT64,
		lm_search_purchases_1d INT64,
		-- afternoon search metrics
		an_search_visits_with_imp INT64,
		an_search_impressions INT64,
		an_search_clicks INT64,
		an_search_purchases_1d INT64,
		-- evening search metrics
		eve_search_visits_with_imp INT64,
		eve_search_impressions INT64,
		eve_search_clicks INT64,
		eve_search_purchases_1d INT64,
		-- search characteristics
		query_session_count INT64,
		search_total_results FLOAT64,
		classified_taxonomy_id INT64,
		query_leaf_category STRING,
		query_top_category STRING,
		query_group_label STRING,
		query_group_label_conf_score FLOAT64
)
PARTITION BY event_date_est;

DELETE FROM `etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance` where event_date_est = active_date


-- create a table that grabs all of the prolist_ranking_signals event. this event fires whenever
-- a request for ads is made on any page. These events will be used to ID the number of empty
-- query sessions, and the number of results that are returned per request.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.ads_query_details`
  as (
select
	distinct
	a.visit_id,
	a.is_possible_bot,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "query") as query,
	-- (select value from unnest(properties.map) where key = "predCtr") as pred_ctr,
	(select value from unnest(properties.map) where key = "page_type") as page_type,
	(select value from unnest(properties.map) where key = "listing_id") as prolist_listings
	-- (select value from unnest(properties.map) where key = "liveBudgetRemaining") as live_budget_remaining
from
	`etsy-visit-pipe-prod.canonical.visits_recent` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_ranking_signals"
where
	date(datetime(timestamp_millis(b.epoch_ms),"America/New_York")) = current_date() - 2 and
	date(datetime(_PARTITIONTIME,"America/New_York")) >= current_date() - 2 - 1
)
;


-- next, grab all of the events for prolist impressions, with some of their logging info.
-- this data will be joined with our click and purchase attribution tables to get summary stats
-- for each query.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_prolist_imps`
  as (
select
	a.visit_id,
	a.is_possible_bot,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "listing_id") as listing_id,	
	(select value from unnest(properties.map) where key = "nonce") as nonce,
	(select value from unnest(properties.map) where key = "logging_key") as logging_key,	
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "page_type") as page_type
	-- (select value from unnest(properties.map) where key = "page_type") as page_type,
	-- (select value from unnest(properties.map) where key = "logging_key") as logging_key
from
	`etsy-visit-pipe-prod.canonical.visits_recent` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_imp_full"
where
	date(datetime(_PARTITIONTIME,"America/New_York")) = current_date - 2 and
	date(datetime(_PARTITIONTIME,"America/New_York")) >= current_date() - 2 - 1
)
;

select
	*
from
	`etsy-data-warehouse-dev.pdavidoff.query_prolist_imps`
limit 50;
select
	count(case when is_possible_bot is true then event_time end)/count(event_time)
from
	`etsy-data-warehouse-dev.pdavidoff.ads_query_details`
;
-- transform the raw ranking signal table into a table with the number of events and categorize
-- them into page group. this table also identifies empty ad requests and the number of results per ad request.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.total_ads_query_sessions`
  as (
select
	date(datetime(event_time, "America/New_York")) as event_date_est,
	extract(hour from datetime(event_time, "America/New_York")) as event_hour_est,
	query,
	visit_id,
	page_type,
	case 
		when page_type = "0" then "Search"
		when page_type = "1" then "Market"
		when page_type = "2" then "Category"
		when page_type in ("4","5","6","7","8","9") then "Listing"
		else "Other"
	end as page_type_group,
	prolist_listings,
	case 
		when prolist_listings = "[]" then 0 
		else (length(prolist_listings) - length(regexp_replace(prolist_listings,",","")))+1 
	end as query_result_count,
	case when prolist_listings = "[]" then 1 else 0 end empty_ads_request
from
	`etsy-data-warehouse-dev.pdavidoff.ads_query_details`
)
;


-- join the raw prolist impression table with prolist attribution tables to get clicks, purchases, and gms
-- for each query. this table will be used for the query/page type table and the hourly table
-- end up with a row for every impression (nonce should be distinct for every impression).
-- change the timezone to EST to align with when the model resets daily (12AM EST).
-- note that "query" is null for category page type.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.click_purchase_merge`
  as (
select
	date(datetime(event_time, "America/New_York")) as event_date_est,
	extract(hour from datetime(event_time, "America/New_York")) as event_hour_est,
	a.query,	
	a.visit_id,
	a.listing_id,
	a.nonce,
	case 
		when a.page_type = "0" then "Search"
		when a.page_type = "1" then "Market"
		when a.page_type = "2" then "Category"
		when a.page_type in ("4","5","6","7","8","9") then "Listing"
		else "Other"
	end as page_type_group,
	count(case when b.nonce is not null then a.nonce end) as clicks,
	sum(b.cost/100) as cost,
	count(case when c.nonce is not null then a.nonce end) as orders_1d,
	sum(c.revenue/100) as revenue_1d
from
	`etsy-data-warehouse-dev.pdavidoff.query_prolist_imps` a left join
	`etsy-data-warehouse-prod.etsy_shard.prolist_click_log` b on a.nonce = b.nonce left join
	`etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` c on b.nonce = c.nonce and 
	datetime(timestamp_seconds(c.purchase_date)) BETWEEN datetime(timestamp_seconds(c.click_date)) AND date_add(datetime(timestamp_seconds(c.click_date)), interval 1 DAY)
group by 1,2,3,4,5,6,7
)
;


-- the below tables will be used in a query/page_type table for each day.
-- there are tables for prolist ranking requests, prolist impressions, listing/shop history and search data.
-- for ranking requests and impressions, break key metrics into time of day chunks  (early morning, late morning, afternoon, evening),
-- so we can see how metrics change over the course of the day. generally, relevance (CTR), and ranking events with empty results decline as budget gets exhausted during the day

-- create a table for empty query sessions which will be used in the query/page type rollup.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_sessions_empty`
  as (
select
	event_date_est,
	query,
	page_type_group,
	count(visit_id) as total_ads_queries,
	count(case when empty_ads_request = 1 then query_result_count end) as total_ads_empty_queries,
	sum(query_result_count) as total_ads_result_count,
	count(case when event_hour_est between 0 and 5 then visit_id end) as em_ads_queries,
	count(case when event_hour_est between 0 and 5 and empty_ads_request = 1 then visit_id end) as em_ads_empty_queries,
	sum(case when event_hour_est between 0 and 5 then query_result_count end) as em_ads_result_count,
	count(case when event_hour_est between 6 and 11 then visit_id end) as lm_ads_queries,
	count(case when event_hour_est between 6 and 11 and empty_ads_request = 1 then visit_id end) as lm_ads_empty_queries,
	sum(case when event_hour_est between 6 and 11 then query_result_count end) as lm_ads_result_count,
	count(case when event_hour_est between 12 and 17 then visit_id end) as an_ads_queries,
	count(case when event_hour_est between 12 and 17 and empty_ads_request = 1 then visit_id end) as an_ads_empty_queries,
	sum(case when event_hour_est between 12 and 17 then query_result_count end) as an_ads_result_count,
	count(case when event_hour_est between 18 and 23 then visit_id end) as eve_ads_queries,
	count(case when event_hour_est between 18 and 23 and empty_ads_request = 1 then visit_id end) as eve_ads_empty_queries,
	sum(case when event_hour_est between 18 and 23 then query_result_count end) as eve_ads_result_count
from
	`etsy-data-warehouse-dev.pdavidoff.total_ads_query_sessions`
group by 1,2,3
order by query,page_type_group
)
;


-- create a query, page level table for attribution metrics. this will be used in the query level rollup.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_clicks_purchases`
  as (
select
	event_date_est,
	query,
	page_type_group,
	-- overall metrics
	count(distinct visit_id) as total_ads_visits_with_imp,
	count(nonce) as total_ads_impressions,
	sum(clicks) as total_ads_clicks,
	sum(cost) as total_ads_cost,
	sum(orders_1d) as total_ads_orders,
	sum(revenue_1d) as total_ads_gms,
	-- early morning total metrics
	count(distinct case when event_hour_est between 0 and 5 then visit_id end) as em_ads_visits_with_imp,
	count(case when event_hour_est between 0 and 5 then nonce end) as em_ads_impressions,
	sum(case when event_hour_est between 0 and 5 then clicks end) as em_ads_clicks,
	sum(case when event_hour_est between 0 and 5 then cost end) as em_ads_cost,
	sum(case when event_hour_est between 0 and 5 then orders_1d end) as em_ads_purchases_1d,
	sum(case when event_hour_est between 0 and 5 then revenue_1d end) as em_ads_gms_1d,
	-- late morning total metrics
	count(distinct case when event_hour_est between 6 and 11 then visit_id end) as lm_ads_visits_with_imp,
	count(case when event_hour_est between 6 and 11 then nonce end) as lm_ads_impressions,
	sum(case when event_hour_est between 6 and 11 then clicks end) as lm_ads_clicks,
	sum(case when event_hour_est between 6 and 11 then cost end) as lm_ads_cost,
	sum(case when event_hour_est between 6 and 11 then orders_1d end) as lm_ads_purchases_1d,
	sum(case when event_hour_est between 6 and 11 then revenue_1d end) as lm_ads_gms_1d,
	-- afternoon
	count(distinct case when event_hour_est between 12 and 17 then visit_id end) as an_ads_visits_with_imp,
	count(case when event_hour_est between 12 and 17 then nonce end) as an_ads_impressions,
	sum(case when event_hour_est between 12 and 17 then clicks end) as an_ads_clicks,
	sum(case when event_hour_est between 12 and 17 then cost end) as an_ads_cost,
	sum(case when event_hour_est between 12 and 17 then orders_1d end) as an_ads_purchases_1d,
	sum(case when event_hour_est between 12 and 17 then revenue_1d end) as an_ads_gms_1d,
	-- evening
	count(distinct case when event_hour_est between 18 and 23 then visit_id end) as eve_ads_visits_with_imp,
	count(case when event_hour_est between 18 and 23 then nonce end) as eve_ads_impressions,
	sum(case when event_hour_est between 18 and 23 then clicks end) as eve_ads_clicks,
	sum(case when event_hour_est between 18 and 23 then cost end) as eve_ads_cost,
	sum(case when event_hour_est between 18 and 23 then orders_1d end) as eve_ads_purchases_1d,
	sum(case when event_hour_est between 18 and 23 then revenue_1d end) as eve_ads_gms_1d
from
	`etsy-data-warehouse-dev.pdavidoff.click_purchase_merge`
group by 1,2,3
order by query,page_type_group
)
;

-- get some data to tell us a bit more about the impressions that buyers see.
-- this table identifies the number of cold start listings (listings w/o an imp in prior 30 days),
-- along with shop level data about the shop budget, spend, and budget constrained status.
-- reference timestamp in the snapshot tables are from the day the events happened,
-- so 30 days look between n-30 and n-1 to get 30 days prior to the date of the impression.
-- output of this table is a listing level table for the date.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.prolist_listing_history`
  as (
with distinct_listings as (
select
	distinct
	event_date_est,
	cast(listing_id as int64) as listing_id
from
	`etsy-data-warehouse-dev.pdavidoff.click_purchase_merge`
where
	event_date_est = current_date - 2
order by listing_id
),cs_listing_shop_history as (
select
	a.event_date_est,
	a.listing_id,
	b.shop_id,
	date(timestamp_seconds(original_create_date)) as listing_create_date,
	case when date_diff(event_date_est,date(timestamp_seconds(original_create_date)),day) <= 7 then 1 else 0 end as new_listing_7d,
	case when date_diff(event_date_est,date(timestamp_seconds(original_create_date)),day) <= 30 then 1 else 0 end as new_listing_30d,
	e.top_shop_status,
	e.power_shop_status,
	sum(c.impression_count) as listing_impressions_30d,
	sum(d.impression_count) as shop_impressions_30d
from
	distinct_listings a left join
	`etsy-data-warehouse-prod.etsy_shard.listings` b on a.listing_id = b.listing_id left join
	`etsy-data-warehouse-prod.etsy_shard.shop_stats_prolist_snapshot_daily` c on a.listing_id = c.listing_id and date(datetime(timestamp_seconds(c.reference_timestamp),"America/New_York")) between event_date_est - 30 and event_date_est - 1 left join
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` d on b.shop_id = d.shop_id and d.date between event_date_est - 30 and event_date_est - 1 left join
	`etsy-data-warehouse-prod.rollups.seller_basics` e on b.shop_id = e.shop_id
group by 1,2,3,4,5,6,7,8
order by a.listing_id
)
select
	a.event_date_est,
	a.listing_id,
	a.shop_id,
	case when listing_impressions_30d > 0 then 0 else 1 end as cold_start_listing,
	case when shop_impressions_30d > 0 then 0 else 1 end as cold_start_shop,
	a.listing_create_date,
	top_shop_status,
	power_shop_status,
	a.new_listing_7d,
	a.new_listing_30d,
	b.budget as shop_budget,
	b.spend as shop_spend,
	b.budget_constrained_shop
from
	cs_listing_shop_history a left join
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` b on a.shop_id = b.shop_id and a.event_date_est = b.date
where
	a.listing_id is not null and event_date_est = "2022-02-03"
)
;


-- append listing level data to the impression level table, and
-- group it to the date, query, page type level
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_page_listing_history_pre`
  as (
with listing_history_join as (
	select
		a.*,
		b.shop_id,
		b.cold_start_listing,
		b.cold_start_shop,
		b.listing_create_date,
		b.new_listing_7d,
		b.new_listing_30d,
		b.shop_budget,
		b.shop_spend,
		b.budget_constrained_shop,
		b.top_shop_status,
		b.power_shop_status
	from
		`etsy-data-warehouse-dev.pdavidoff.click_purchase_merge` a left join
		`etsy-data-warehouse-dev.pdavidoff.prolist_listing_history` b on cast(a.listing_id as int64) = b.listing_id
	)
select
	event_date_est,
	query,
	page_type_group,
	-- count(*) as total_impressions,
	count(distinct listing_id) as distinct_listing_ads_imp,
	count(distinct shop_id) as distinct_shops_ads_imp,
	sum(shop_budget) as ads_budget_imp,
	sum(shop_spend) as ads_spend_imp,
	sum(budget_constrained_shop) as budget_constrained_shop_ads_imp,
	sum(cold_start_listing) as cs_listing_ads_imp,
	sum(cold_start_shop) as cs_seller_ads_imp,
	sum(new_listing_7d) as new_listing_7d_ads_imp,
	sum(new_listing_30d) as new_listing_30d_ads_imp,
	sum(top_shop_status) as top_shop_ads_imp,
	sum(power_shop_status) as power_shop_ads_imp
from
	listing_history_join
group by 1,2,3
order by event_date_est,query,page_type_group
)
;


-- pull in search data by query for comparison with ads data performance.
-- the search tables are in GMT, so use the start time of the buyer visit to set the hour for
-- impression, click and purchase data.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.hourly_query_search_data`
  as (
select
	date(b.start_datetime,"America/New_York") as event_date_est,
	extract(hour from datetime(b.start_datetime,"America/New_York")) as event_hour_est,
	query,
	case 
		when page = "search" then "Search"
		when page = "market" then "Market"
		when page = "category_page" then "Category"
	end as page_type_group,
	a.visit_id,
	listing_id,
	sum(impressions) as search_impressions,
	sum(clicks) as search_clicks,
	sum(purchases) as search_purchases
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions` a join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date >= current_date - 2 - 1 and date(datetime(b.start_datetime,"America/New_York")) = current_date - 2
where
	a._date >= current_date - 2 - 1
group by 1,2,3,4,5,6
)
;


-- create a table that breaks out search data into same chunks as we have for prolist listings.
-- this will be used to compare click / purchase rates across the chunks
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.search_hour_chunk_data`
  as (
  	select
	event_date_est,
	query,
	page_type_group,
	-- overall metrics
	count(distinct visit_id) as total_search_visits_with_imp,
	sum(search_impressions) as total_search_impressions,
	sum(search_clicks) as total_search_clicks,
	sum(search_purchases) as total_search_purchases,
	-- early morning
	count(distinct case when event_hour_est between 0 and 5 then visit_id end) as em_search_visits_with_imp,
	sum(case when event_hour_est between 0 and 5 then search_impressions end) as em_search_impressions,
	sum(case when event_hour_est between 0 and 5 then search_clicks end) as em_search_clicks,
	sum(case when event_hour_est between 0 and 5 then search_purchases end) as em_search_purchases_1d,
	-- late morning
	count(distinct case when event_hour_est between 6 and 11 then visit_id end) as lm_search_visits_with_imp,
	count(case when event_hour_est between 6 and 11 then search_impressions end) as lm_search_impressions,
	sum(case when event_hour_est between 6 and 11 then search_clicks end) as lm_search_clicks,
	sum(case when event_hour_est between 6 and 11 then search_purchases end) as lm_search_purchases_1d,
	-- afternoon
	count(distinct case when event_hour_est between 12 and 17 then visit_id end) as an_search_visits_with_imp,
	count(case when event_hour_est between 12 and 17 then search_impressions end) as an_search_impressions,
	sum(case when event_hour_est between 12 and 17 then search_clicks end) as an_search_clicks,
	sum(case when event_hour_est between 12 and 17 then search_purchases end) as an_search_purchases_1d,
	-- evening
	count(distinct case when event_hour_est between 18 and 23 then visit_id end) as eve_search_visits_with_imp,
	count(case when event_hour_est between 18 and 23 then search_impressions end) as eve_search_impressions,
	sum(case when event_hour_est between 18 and 23 then search_clicks end) as eve_search_clicks,
	sum(case when event_hour_est between 18 and 23 then search_purchases end) as eve_search_purchases_1d,
from
	`etsy-data-warehouse-dev.pdavidoff.hourly_query_search_data`
group by 1,2,3
order by event_date_est,query,page_type_group
)
;



-- this table captures query level data for a few more dimensions, like the classified category and whether the query is
-- broad vs. direct. it also grabs the total number of results and the number search queries.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_level_data`
  as (
with session_data as (
select
	date(datetime(b.start_datetime,"America/New_York")) as event_date_est,
	query,
	query_session_id,
	classified_taxonomy_id,
	a.visit_id,
	max_total_results
from
	`etsy-data-warehouse-prod.search.query_sessions_new` a join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date >= current_date - 3 and date(datetime(b.start_datetime,"America/New_York")) = current_date - 2
where
	a._date >= current_date - 2 - 1
),query_summary_stats as (
select
	event_date_est,
	query,
	count(distinct query_session_id) as query_session_count,
	avg(max_total_results) as max_total_results
from
	session_data
group by 1,2
),taxonomy as (
select
	event_date_est,
	query,
	classified_taxonomy_id,
	count(distinct query_session_id) as query_session_count
from
	session_data
group by 1,2,3
),top_taxonomy as (
select
	distinct
	a.event_date_est,
	a.query,
	a.classified_taxonomy_id,
	b.path as query_leaf_category,
	(split(full_path, ".")[ORDINAL(1)]) as query_top_category
from
	(select *,row_number() over(partition by query order by query_session_count desc) as rn from taxonomy) a join
	`etsy-data-warehouse-prod.structured_data.taxonomy` b on a.classified_taxonomy_id = b.taxonomy_id
where
	rn = 1
),query_intent_labels as (
select
	a.*,
	query_group_label,
	query_group_label_conf_score
from
	top_taxonomy a left join
	(select query_raw,inference.label as query_group_label,inference.confidence as query_group_label_conf_score,row_number() over(partition by query_raw order by inference.confidence desc) as rn from `etsy-data-warehouse-prod.arizona.query_intent_labels`) b on a.query = b.query_raw and b.rn = 1
)
select
	a.event_date_est,
	a.query,
	a.query_session_count,
	a.max_total_results as search_total_results,
	b.classified_taxonomy_id,
	b.query_leaf_category,
	b.query_top_category,
	b.query_group_label,
	b.query_group_label_conf_score
from
	query_summary_stats a left join
	query_intent_labels b on a.query = b.query
)
;


-- create a table that joins impression level data with listing level data
INSERT INTO `etsy-data-warehouse-dev.rollups.etsy_ads_daily_query_performance` (
select
  	a.event_date_est,
  	a.query,
  	a.page_type_group,
  	-- overall ads metrics
  	total_ads_queries,
  	total_ads_empty_queries,
  	total_ads_result_count,
  	total_ads_visits_with_imp,
  	total_ads_impressions,
  	total_ads_clicks,
  	total_ads_cost,
  	total_ads_orders,
  	total_ads_gms,
  	-- early morning ads metrics
		em_ads_queries,
		em_ads_empty_queries,
		em_ads_result_count,
		em_ads_visits_with_imp,
		em_ads_impressions,
		em_ads_clicks,
		em_ads_cost,
		em_ads_purchases_1d,
		em_ads_gms_1d,
		-- late morning ads metrics
		lm_ads_queries,
		lm_ads_empty_queries,
		lm_ads_result_count,
		lm_ads_visits_with_imp,
		lm_ads_impressions,
		lm_ads_clicks,
		lm_ads_cost,
		lm_ads_purchases_1d,
		lm_ads_gms_1d,
		-- afternoon ads metrics
		an_ads_queries,
		an_ads_empty_queries,
		an_ads_result_count,
		an_ads_visits_with_imp,
		an_ads_impressions,
		an_ads_clicks,
		an_ads_cost,
		an_ads_purchases_1d,
		an_ads_gms_1d,
		-- eve ads metrics
		eve_ads_queries,
		eve_ads_empty_queries,
		eve_ads_result_count,
		eve_ads_visits_with_imp,
		eve_ads_impressions,
		eve_ads_clicks,
		eve_ads_cost,
		eve_ads_purchases_1d,
		eve_ads_gms_1d,
		-- ads listing characteristics
		distinct_listing_ads_imp,
		distinct_shops_ads_imp,
		ads_budget_imp,
		ads_spend_imp,
		budget_constrained_shop_ads_imp,
		cs_listing_ads_imp,
		cs_seller_ads_imp,
		new_listing_7d_ads_imp,
		new_listing_30d_ads_imp,
		top_shop_ads_imp,
		power_shop_ads_imp,
  	-- overall search metrics
		total_search_visits_with_imp,
		total_search_impressions,
		total_search_clicks,
		total_search_purchases,
		-- early morning search metrics
		em_search_visits_with_imp,
		em_search_impressions,
		em_search_clicks,
		em_search_purchases_1d,
		-- late morning search metrics
		lm_search_visits_with_imp,
		lm_search_impressions,
		lm_search_clicks,
		lm_search_purchases_1d,
		-- afternoon search metrics
		an_search_visits_with_imp,
		an_search_impressions,
		an_search_clicks,
		an_search_purchases_1d,
		-- evening search metrics
		eve_search_visits_with_imp,
		eve_search_impressions,
		eve_search_clicks,
		eve_search_purchases_1d,
		-- search characteristics
		query_session_count,
		search_total_results,
		classified_taxonomy_id,
		query_leaf_category,
		query_top_category,
		query_group_label,
		query_group_label_conf_score
from
	`etsy-data-warehouse-dev.pdavidoff.query_clicks_purchases` a left join
	`etsy-data-warehouse-dev.pdavidoff.query_sessions_empty` b on a.query = b.query and a.event_date_est = b.event_date_est and a.page_type_group = b.page_type_group left join
	`etsy-data-warehouse-dev.pdavidoff.query_page_listing_history` c on a.query = c.query and a.event_date_est = c.event_date_est and a.page_type_group = c.page_type_group left join
	`etsy-data-warehouse-dev.pdavidoff.search_hour_chunk_data` d on a.query = d.query and a.event_date_est = d.event_date_est and a.page_type_group = d.page_type_group left join
	`etsy-data-warehouse-dev.pdavidoff.query_level_data` e on a.query = e.query
)
;

