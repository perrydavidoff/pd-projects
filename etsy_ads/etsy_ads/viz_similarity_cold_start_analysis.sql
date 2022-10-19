-- date: 1/3/2022
-- author: pdavidoff
-- Overview: analysis into image/listing embedding experiment which ran in late October. spend as up in the
-- images variant, but this analysis looks into drivers of that change, specifically around cold start.

-- create a table for the analysis. this table has impressions, clicks, spend, attribution during the experiment
-- 	and grabs some historical data at the listing and shop level to identify cold start
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
  as (
with base as ( 
select
	(split(visit_id, ".")[ORDINAL(1)]) as browser_id,
	visit_id,
	_date as date,
	ab_variant,
	sequence_number
from
	`etsy-data-warehouse-prod.catapult.ab_tests`
where
	ab_test = "ads.prolist.concat_embeddings_similarity" and
	_date between "2021-10-26" and "2021-11-02"
-- group by 1
),imps as (
select
	a.browser_id,
	a.visit_id,
	a.date,
	a.ab_variant,
	-- click,
	-- add_cart,
	-- purchase,
	timestamp_seconds(cast(timestamp as int64)) as imp_time,
	query,
	listing_id,
	shop_id,
	cost,
	predCtr as pred_ctr,
	page_type,
	prolist_page,
	prolist_platform,
	prolist_query_type,
	predCvr as pred_cvr,
	embeddingsSimilarity as embed_similarity,
	logging_key,
	referringListingId as source_listing_id
from
	base a left join
	`etsy-prolist-etl-prod.prolist.attributed_impressions` b on a.visit_id = b.visit_id and extract(date from _PARTITIONTIME) between "2021-10-26" and "2021-11-01"
order by listing_id
),perf as (
select
	a.*,
	case when b.nonce is not null then 1 else 0 end as click,
	b.cost/100 as spend,
	case when c.nonce is not null then 1 else 0 end as purchase,
	c.revenue/100 as ads_gms
from
	imps a left join
	`etsy-data-warehouse-prod.etsy_shard.prolist_click_log` b on a.logging_key = b.plkey and timestamp_seconds(b.click_date) between "2021-10-26" and "2021-11-01" left join
	`etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` c on b.plkey = c.plkey and extract(date from timestamp_seconds(purchase_date)) between extract(date from timestamp_seconds(b.click_date)) and extract(date from timestamp_seconds(b.click_date)) + 30
order by listing_id,shop_id
),listing_first_date as (
select
	listing_id,
	shop_id,
	min(date) as first_visit_date
from
	perf
group by 1,2
),listing_history as (
select
	a.listing_id,
	a.first_visit_date,
	extract(date from timestamp_seconds(original_create_date)) as listing_create_date,
	price_usd,
	d.taxonomy_id as listing_taxonomy_id,
	d.top_category as listing_top_category,
	-- e.taxonomy_id as source_listing_taxonomy_id,
	-- e.top_category as source_listing_top_category,
	max(case when b.listing_id is not null then 1 else 0 end) as imp_8w,
	count(*) as total_imps_8w,
	max(case when b.listing_id is not null and click > 0 then 1 else 0 end) as click_8w,
	count(case when b.listing_id is not null and click > 0 then b.listing_id end) as total_clicks_8w
from
	listing_first_date a left join
	`etsy-prolist-etl-prod.prolist.attributed_impressions` b on a.listing_id = b.listing_id and extract(date from _PARTITIONTIME) between first_visit_date - 60 and first_visit_date - 1 and extract(date from _PARTITIONTIME) between "2021-08-25" and "2021-11-01" left join
	`etsy-data-warehouse-prod.listing_mart.listings` c on a.listing_id = c.listing_id left join
	`etsy-data-warehouse-prod.listing_mart.listing_attributes` d on c.listing_id = d.listing_id
group by 1,2,3,4,5,6
order by listing_id
),shop_first_date as (
select
	shop_id,
	min(date) as first_shop_date
from
	perf
group by 1
),shop_history as (
select
	a.shop_id,
	a.first_shop_date,
	b.impressions_last_4w,
	b.spend_last_4w,
	b.budget as prior_day_budget,
	b.country,
	c.seller_tier,
	c.top_category_new
from
	shop_first_date a left join
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` b on a.shop_id = b.shop_id and b.date = first_shop_date - 1 left join
	`etsy-data-warehouse-prod.rollups.seller_basics` c on a.shop_id = c.shop_id
order by shop_id
)
select
	a.*,
	b.listing_create_date,
	b.price_usd,
	b.imp_8w as listing_imp_8w,
	b.total_imps_8w as listing_total_imps_8w,
	b.click_8w as listing_click_8w,
	b.total_clicks_8w as listing_total_clicks_8w,
	c.impressions_last_4w as shop_imps_4w,
	c.spend_last_4w as shop_spend_4w,
	c.prior_day_budget as shop_prior_day_budget,
	c.country as shop_country,
	c.seller_tier,
	c.top_category_new as shop_category,
	b.listing_top_category,
	b.listing_taxonomy_id,
	d.top_category as source_listing_top_category,
	d.taxonomy_id as source_listing_taxonomy_id
from
	perf a left join
	listing_history b on a.listing_id = b.listing_id left join
	shop_history c on a.shop_id = c.shop_id left join
	`etsy-data-warehouse-prod.listing_mart.listing_attributes` d on a.source_listing_id = d.listing_id
)
;


-- overall metrics. can see the lift in spend (0.4%) in the images variant
-- from higher CTR (0.84%)
select
	ab_variant,
	count(distinct browser_id) as browser_count,
	count(*)/count(distinct browser_id) as imps_per_browser,
	count(case when click = 1 then listing_id end)/count(listing_id) as ctr,
	sum(case when click = 1 then spend end)/count(case when click = 1 then listing_id end) as cpc,
	sum(spend)/count(distinct browser_id) as spend_per_browser,
	count(distinct case when click = 1 and purchase = 1 then listing_id end)/count(distinct case when click = 1 then listing_id end) as pccvr,
	sum(case when click = 1 and purchase = 1 then ads_gms end)/count(case when click = 1 and purchase = 1 then listing_id end) as gms_per_convert,
	sum(ads_gms)/sum(spend) as roas
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
group by 1
order by 1 desc
;

-- just listing page for overall metrics. much higher impacts across the board
-- because we're removing pages that diluted effects
select
	ab_variant,
	count(distinct browser_id) as browser_count,
	count(*)/count(distinct browser_id) as imps_per_browser,
	count(case when click = 1 then listing_id end)/count(listing_id) as ctr,
	sum(case when click = 1 then spend end)/count(case when click = 1 then listing_id end) as cpc,
	sum(spend)/count(distinct browser_id) as spend_per_browser,
	count(case when click = 1 and purchase = 1 then listing_id end)/count(case when click = 1 then listing_id end) as pccvr,
	sum(case when click = 1 and purchase = 1 then ads_gms end)/count(case when click = 1 and purchase = 1 then listing_id end) as gms_per_convert,
	sum(ads_gms)/sum(spend) as roas
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8
group by 1
order by 1 desc
;

-- now onto cold start. let's learn a bit about cold start listings and their impact on key metrics
select
	case when listing_click_8w = 0 then "Cold Start" else "Other" end as listing_type,
	count(listing_id)/sum(count(listing_id)) over() as impression_share,
	count(*)/count(distinct browser_id) as imps_per_browser,
	count(case when click = 1 then listing_id end)/count(listing_id) as ctr,
	sum(case when click = 1 then spend end)/count(case when click = 1 then listing_id end) as cpc,
	sum(spend)/count(distinct browser_id) as spend_per_browser,
	count(case when click = 1 and purchase = 1 then listing_id end)/count(case when click = 1 then listing_id end) as pccvr,
	sum(case when click = 1 and purchase = 1 then ads_gms end)/count(case when click = 1 and purchase = 1 then listing_id end) as gms_per_convert,
	sum(ads_gms)/sum(spend) as roas
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8 and ab_variant = "off"
group by 1
order by 1
;

-- cold start listing prevalence. looks like all of the variants lowered
-- the number of listings that were cold start
select
	ab_variant,
	count(distinct browser_id) as browser_count,
	count(listing_id) as imps,
	count(case when listing_imp_4w = 0 then listing_id end)/count(listing_id) as cs_imp_listing_imp_share,
	count(case when listing_click_4w = 0 then listing_id end)/count(listing_id) as cs_click_listing_imp_share,
	count(case when shop_imps_4w is null then listing_id end)/count(listing_id) as cs_imp_shop_imp_share,
	count(case when shop_spend_4w is null then listing_id end)/count(listing_id) as cs_spend_shop_imp_share
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
group by 1
order by 1
;

-- let's look just at the listing page. looks like cold start impression 
-- and click share decreased with this experiment. 
select
	ab_variant,
	count(distinct browser_id) as browser_count,
	count(listing_id) as imps,
	count(case when listing_click_8w = 0 then listing_id end)/count(listing_id) as cs_listing_imp_share,
	count(case when listing_click_8w = 0 and click = 1 then listing_id end)/count(case when click = 1 then listing_id end) as cs_listing_click_share,
	sum(case when listing_click_8w = 0 and click = 1 then spend end)/sum(case when click = 1 then spend end) as cs_listing_spend_share,
	count(case when listing_click_8w = 0 and purchase = 1 then listing_id end)/count(case when purchase = 1 then listing_id end) as cs_listing_purchase_share,
	sum(case when listing_click_8w = 0 and purchase = 1 then ads_gms end)/sum(case when purchase = 1 then ads_gms end) as cs_listing_gms_share
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8
group by 1
order by 1 desc
;

-- category and listing view by cold start
-- likelihood that the impression had the same category as source listing
-- was consistent across all variants and categories
-- category match
select
	-- source_listing_top_category,
	count(case when ab_variant = "off" then listing_id end) as off_imps,
	count(case when ab_variant = "off" and listing_taxonomy_id = source_listing_taxonomy_id then listing_id end)/count(case when ab_variant = "off" then listing_id end) as off_top_category_match,
	count(case when ab_variant = "listings" and listing_taxonomy_id = source_listing_taxonomy_id then listing_id end)/count(case when ab_variant = "listings" then listing_id end) as listings_top_category_match,
	count(case when ab_variant = "images" and listing_taxonomy_id = source_listing_taxonomy_id then listing_id end)/count(case when ab_variant = "images" then listing_id end) as images_top_category_match,
	count(case when ab_variant = "image_and_listings" and listing_taxonomy_id = source_listing_taxonomy_id then listing_id end)/count(case when ab_variant = "image_and_listings" then listing_id end) as image_and_listings_top_category_match
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8 and source_listing_id is not null
-- group by 1
-- order by 2 desc
;

select
	source_listing_top_category,
	count(case when ab_variant = "off" then listing_id end) as off_imps,
	count(case when ab_variant = "off" and listing_top_category = source_listing_top_category then listing_id end)/count(case when ab_variant = "off" then listing_id end) as off_top_category_match,
	count(case when ab_variant = "listings" and listing_top_category = source_listing_top_category then listing_id end)/count(case when ab_variant = "listings" then listing_id end) as listings_top_category_match,
	count(case when ab_variant = "images" and listing_top_category = source_listing_top_category then listing_id end)/count(case when ab_variant = "images" then listing_id end) as images_top_category_match,
	count(case when ab_variant = "image_and_listings" and listing_top_category = source_listing_top_category then listing_id end)/count(case when ab_variant = "image_and_listings" then listing_id end) as image_and_listings_top_category_match
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8 and source_listing_id is not null
group by 1
order by 2 desc
;


-- remaining cold start listings. listings that remained in the 
-- embeddings variants were lower priced, less likely to be new, 
-- and more likely to come from top sellers.
select
	ab_variant,
	avg(price_usd/100) as avg_listing_price,
	approx_quantiles(price_usd/100,100)[OFFSET(50)] as median_price,
	avg(cost/100) as avg_bid_price,
	approx_quantiles(cost/100,100)[OFFSET(50)] as median_bid,
	avg(case when pred_ctr >= 0 then pred_ctr end) as avg_pred_ctr,
	avg(case when pred_cvr >= 0 then pred_cvr end) as avg_pred_cvr,
	count(case when date_diff(date,listing_create_date,day) <= 7 then listing_id end)/count(listing_id) as new_listing_share,
	count(case when source_listing_taxonomy_id = listing_taxonomy_id then listing_id end)/count(listing_id) as listing_category_match,
	count(case when seller_tier in ("top seller","power seller") then listing_id end)/count(listing_id) as top_seller_share,
	count(case when shop_spend_4w = 0 then listing_id end)/count(listing_id) as cold_start_shop_rate,
	count(case when coalesce(shop_prior_day_budget,0) = 0 then listing_id end)/count(listing_id) as share_of_prior_budget_zero
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8 and listing_click_8w = 0
group by 1
order by 1 desc
;

-- by date - did cold start listing impression share decrease
-- over the course of the experiment in the treatments?
select
	date,
	count(case when ab_variant = "off" and listing_click_8w = 0 then listing_id end)/count(case when ab_variant = "off" then listing_id end) as off_cold_start_listings,
	count(case when ab_variant = "listings" and listing_click_8w = 0 then listing_id end)/count(case when ab_variant = "listings" then listing_id end) as listings_cold_start,
	count(case when ab_variant = "images" and listing_click_8w = 0 then listing_id end)/count(case when ab_variant = "images" then listing_id end) as images_cold_start,
	count(case when ab_variant = "image_and_listings" and listing_click_8w = 0 then listing_id end)/count(case when ab_variant = "image_and_listings" then listing_id end) as img_and_listings_cold_start
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8
group by 1
order by 1
;

-- look at embedding similarity score to see the share of impressions
-- that had a default score
select
	-- ab_variant,
	count(case when ab_variant = "images" and embed_similarity = 0.69 then listing_id end)/count(case when ab_variant = "images" then listing_id end) as image_default_score,
	count(case when ab_variant = "listings" and embed_similarity = 0.7 then listing_id end)/count(case when ab_variant = "listings" then listing_id end) as listings_default_score
from
	`etsy-data-warehouse-dev.pdavidoff.embed_similarity_exp_cold_start`
where
	page_type = 8 and listing_click_8w = 1
-- group by 1
-- order by 1 desc
;

