-- Etsy Ads long term strategy adoption needs
-- Date: November, 2021
-- Overview: this part of the analysis looks into adoption targets we need to hit to help us meet opportunities outlined
-- in the 2022 - 2025 strategy

-- let's get budget for seller tiers and call out if they're constrained or not
with base as (
select
	a.shop_id,
	a.seller_tier,
	sum(spend)/sum(budget) as budget_util,
	sum(revenue) as total_revenue,
	sum(spend) as total_spend,
	sum(budget) as total_budget,
	sum(impression_count) as impression_count,
	sum(click_count) as click_count
from
	`etsy-data-warehouse-prod.rollups.seller_basics` a left join
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` b on a.shop_id = b.shop_id and date >= current_date - 30
where
	active_seller_status = 1
group by 1,2
)
select
	seller_tier,
	case when budget_util >= 0.9 then 1 else 0 end as constrained,
	count(distinct shop_id) as shop_count,
	count(distinct case when total_budget > 0 then shop_id end) as ea_sellers,
	sum(total_revenue) as total_revenue,
	sum(total_spend) as total_spend,
	sum(total_budget) as total_budget,
	sum(impression_count) as impression_count,
	sum(click_count) as click_count
from
	base
group by 1,2
order by 1,2
;

-- have constrained sellers raised budget recently?
with constrained_sellers as (
select
	shop_id,
	sum(spend)/sum(budget) as budget_util
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
where
	date between "2021-09-26" and "2021-10-26"
group by 1
having sum(spend)/sum(budget) >= 0.9
),spend_trends as (
select
	date,
	count(distinct shop_id) as shop_count,
	sum(spend) as total_spend,
	sum(budget) as total_budget,
	sum(revenue) as total_revenue,
	sum(click_count) as toatl_clicks,
	sum(impression_count) as total_impressions
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
where
	date >= "2021-09-26" and shop_id in (select shop_id from constrained_sellers)
group by 1
),base4 as (
select
	a.*,
	b.spend as total_spend,
	b.etsy_visits as total_etsy_visits,
	b.etsy_visits/b.ly_etsy_visits - 1 as etsy_visits_yy,
	b.adjusted_budget/b.ly_adjusted_budget - 1 as adj_budget_yy
from
	spend_trends a join
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary` b on a.date = b.date
order by date
),attr_extract as (
select
	extract(date from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as date,
	extract(hour from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as hour,
	count(*) as impression_count,
	count(case when click = 1 then logging_key end) as clicks,
	count(case when click = 1 then logging_key end)/count(*) as ctr
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	extract(date from _PARTITIONTIME) >= "2021-09-26"
group by 1,2
),ctr_over_day as (
select
	date,
	sum(case when hour = 0 then clicks end)/sum(case when hour = 0 then impression_count end) as start_ctr,
	sum(case when hour = 4 then clicks end)/sum(case when hour = 4 then impression_count end) as hour_4_ctr,
	sum(case when hour = 8 then clicks end)/sum(case when hour = 8 then impression_count end) as hour_8_ctr,
	sum(case when hour = 12 then clicks end)/sum(case when hour = 12 then impression_count end) as hour_12_ctr,
	sum(case when hour = 16 then clicks end)/sum(case when hour = 16 then impression_count end) as hour_16_ctr,
	sum(case when hour = 20 then clicks end)/sum(case when hour = 20 then impression_count end) as hour_20_ctr,
	sum(case when hour = 23 then clicks end)/sum(case when hour = 23 then impression_count end) as eod_ctr	
from
	attr_extract
group by 1
)
select
	a.*,
	b.start_ctr,
	b.hour_12_ctr,
	b.eod_ctr
from
	base4 a join
	ctr_over_day b on a.date = b.date
order by date
;

-- matrix constrained sellers and ctr
with base as ()
with base as (
select
	extract(date from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as date,
	extract(hour from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as hour,
	count(*) as impression_count,
	count(case when click = 1 then logging_key end) as clicks,
	count(case when click = 1 then logging_key end)/count(*) as ctr
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	extract(date from _PARTITIONTIME) between current_date - 30 and current_date - 1
group by 1,2
),ctr_over_day as (
select
	date,
	sum(case when hour = 0 then clicks end)/sum(case when hour = 0 then impression_count end) as start_ctr,
	sum(case when hour = 4 then clicks end)/sum(case when hour = 4 then impression_count end) as hour_4_ctr,
	sum(case when hour = 8 then clicks end)/sum(case when hour = 8 then impression_count end) as hour_8_ctr,
	sum(case when hour = 12 then clicks end)/sum(case when hour = 12 then impression_count end) as hour_12_ctr,
	sum(case when hour = 16 then clicks end)/sum(case when hour = 16 then impression_count end) as hour_16_ctr,
	sum(case when hour = 20 then clicks end)/sum(case when hour = 20 then impression_count end) as hour_20_ctr,
	sum(case when hour = 23 then clicks end)/sum(case when hour = 23 then impression_count end) as eod_ctr
from
	base
group by 1
)
select
	a.*,
	b.budget/b.ly_budget - 1 as budget_yy,
	b.adjusted_budget/b.ly_adjusted_budget - 1 as adj_budget_yy,
	b.etsy_visits/b.ly_etsy_visits - 1 as etsy_visits_yy
from
	ctr_over_day a join
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary` b on a.date = b.date
order by date
;


-- has the relevance gap closed since we've added more budget?
with base as (
select
	extract(date from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as date,
	extract(hour from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as hour,
	count(*) as impression_count,
	count(case when click = 1 then logging_key end) as clicks,
	count(case when click = 1 then logging_key end)/count(*) as ctr
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	extract(date from _PARTITIONTIME) between current_date - 30 and current_date - 1
group by 1,2
),ctr_over_day as (
select
	date,
	sum(case when hour = 0 then clicks end)/sum(case when hour = 0 then impression_count end) as start_ctr,
	sum(case when hour = 4 then clicks end)/sum(case when hour = 4 then impression_count end) as hour_4_ctr,
	sum(case when hour = 8 then clicks end)/sum(case when hour = 8 then impression_count end) as hour_8_ctr,
	sum(case when hour = 12 then clicks end)/sum(case when hour = 12 then impression_count end) as hour_12_ctr,
	sum(case when hour = 16 then clicks end)/sum(case when hour = 16 then impression_count end) as hour_16_ctr,
	sum(case when hour = 20 then clicks end)/sum(case when hour = 20 then impression_count end) as hour_20_ctr,
	sum(case when hour = 23 then clicks end)/sum(case when hour = 23 then impression_count end) as eod_ctr
from
	base
group by 1
)
select
	a.*,
	b.budget/b.ly_budget - 1 as budget_yy,
	b.adjusted_budget/b.ly_adjusted_budget - 1 as adj_budget_yy,
	b.etsy_visits/b.ly_etsy_visits - 1 as etsy_visits_yy
from
	ctr_over_day a join
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary` b on a.date = b.date
order by date
;

-- weekly view
with base as (
select
	extract(date from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as date,
	extract(hour from timestamp_seconds(cast(timestamp as int64)) AT TIME ZONE "America/New_York") as hour,
	count(*) as impression_count,
	count(case when click = 1 then logging_key end) as clicks,
	count(case when click = 1 then logging_key end)/count(*) as ctr
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	extract(date from _PARTITIONTIME) between current_date - 365 and current_date - 1
group by 1,2
),ctr_over_day as (
select
	date_trunc(date,week) as week,
	sum(clicks)/sum(impression_count) as ctr,
	sum(case when hour = 0 then clicks end)/sum(case when hour = 0 then impression_count end) as start_ctr,
	sum(case when hour = 4 then clicks end)/sum(case when hour = 4 then impression_count end) as hour_4_ctr,
	sum(case when hour = 8 then clicks end)/sum(case when hour = 8 then impression_count end) as hour_8_ctr,
	sum(case when hour = 12 then clicks end)/sum(case when hour = 12 then impression_count end) as hour_12_ctr,
	sum(case when hour = 16 then clicks end)/sum(case when hour = 16 then impression_count end) as hour_16_ctr,
	sum(case when hour = 20 then clicks end)/sum(case when hour = 20 then impression_count end) as hour_20_ctr,
	sum(case when hour = 23 then clicks end)/sum(case when hour = 23 then impression_count end) as eod_ctr
from
	base
group by 1
),daily_summary as (
select
	date_trunc(date,week) as week,
	sum(adjusted_budget)/7 as adj_budget_per_day,
	sum(etsy_visits)/7 as etsy_visits_per_day,
	sum(adjusted_budget)/sum(ly_adjusted_budget) - 1 as adj_budget_yy,
	sum(etsy_visits/sum(ly_etsy_visits) - 1 as etsy_visits_yy
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary` 
where
	date between current_date - 365 and current_date - 1
group by 1
)
select
	a.*,
	b.adj_budget_yy,
	b.etsy_visits_yy,
	b.adj_budget_per_day,
	b.etsy_visits_per_day
from
	ctr_over_day a join
	daily_summary b on a.week = b.week
order by a.week
;

-- get first date for each seller
create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps`
	as (
with first_prolist_date as (
select
	shop_id,
	min(date) as first_prolist_date
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
group by 1
)
;


-- how quickly do constrained shops get constrained?
-- what is their CTR when they're not constrained?
-- what is the "replacement" CTR? use subcategory? top query?
create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps`
	as (
with base as (
select
	a.shop_id,
	seller_tier,
	top_category_new,
	spend/budget as budget_util,
	spend,
	budget,
	case when spend/budget >= 0.9 then 1 else 0 end as constrained,
	click_count as clicks,
	impression_count as imps,
	revenue
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.shop_id = b.shop_id
where
	date = "2021-11-08"
),base2 as (
select
	a.*,
	visit_id||query as query_visit_session,
	query,
	row_number() over(partition by visit_id||query order by datetime(timestamp_seconds(cast(timestamp as int64)),"America/New_York")) as rank,
	b.listing_id,
	b.click,
	predCtr,
	datetime(timestamp_seconds(cast(timestamp as int64)),"America/New_York") as impression_time,
	extract(hour from timestamp_seconds(cast(timestamp as int64)) at time zone "America/New_York") as impression_hour,
	case when click = 0 then 0 else cost end as cost_adj
from
	base a join
	`etsy-prolist-etl-prod.prolist.attributed_impressions` b on a.shop_id = b.shop_id
where
	extract(date from _PARTITIONTIME) > "2021-11-07" and
	page_type in (0,1,2) and
	extract(date from timestamp_seconds(cast(timestamp as int64)) at time zone "America/New_York") = "2021-11-08"
)
select
	*,
	sum(cost_adj) over(partition by shop_id order by impression_time) as rolling_cost
from
	base2
)
;

-- let's learn more about constrained sellers!
with shop_base as (
select
	date_trunc(date,month) as month,
	shop_id,	
	sum(spend)/sum(budget) as budget_util,
	sum(spend) as total_spend,
	sum(budget) as total_budget,
	sum(click_count)/sum(impression_count) as ctr,
	sum(click_count) as click_count,
	sum(impression_count) as imp_count
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
where
	date >= "2016-01-01"
group by 1,2
),agg as (
select
	month,
	sum(case when budget_util >= 0.9 then total_spend end) as constrained_spend,
	sum(case when budget_util >= 0.9 then total_budget end) as constrained_budget,
	count(distinct case when budget_util >= 0.9 then shop_id end) as constrained_shops
from
	shop_base
group by 1
),summary_stats as (
select
	date_trunc(date,month) as month,
	sum(spend) as total_spend,
	sum(budget) as total_budget,
	sum(adjusted_budget) as total_adj_budget,
	sum(etsy_visits) as total_etsy_visits,
	sum(spend)/sum(ly_spend)-1 as spend_yy,
	sum(budget)/sum(ly_budget)-1 as budget_yy,
	sum(adjusted_budget)/sum(ly_adjusted_budget)-1 as adj_budget_yy,
	sum(etsy_visits)/sum(ly_etsy_visits)-1 as visit_yy,
	(sum(clicks)/sum(impressions))/(sum(ly_clicks)/sum(ly_impressions))-1 as ctr_yy,
	sum(impressions)/sum(ly_impressions)-1 as imp_yy
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary`
where
	date >= "2016-01-01"
group by 1
)
select
	a.*,
	constrained_spend/total_spend as constrained_spend_share,
	constrained_budget/total_budget as constrained_budget_share,
	constrained_budget/total_adj_budget as constrained_budget_share,
	total_etsy_visits,
	spend_yy,
	budget_yy,
	adj_budget_yy,
	visit_yy,
	ctr_yy,
	imp_yy
from
	agg a join
	summary_stats b on a.month = b.month
order by 1
;

-- new budget context
with base as (
select
	shop_id,
	min(date) as first_prolist_date,
	min(case when impression_count > 0 then date end) as first_imp_date,
	min(case when click_count > 0 then date end) as first_click_date
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
where
	date >= "2014-01-01"
group by 1
)
select
	date_trunc(a.date,month) as month,
	count(distinct case when b.shop_id is not null then b.shop_id end)/count(distinct date) as new_sellers_per_day,
	sum(case when b.shop_id is not null then budget end)/count(distinct date) as new_seller_budget_per_day,
	sum(case when b.shop_id is not null then spend end)/count(distinct date) as new_seller_spend_per_day,
	sum(case when b.shop_id is not null then impression_count end)/count(distinct date) as new_seller_imp_per_day,
	sum(case when b.shop_id is not null then click_count end)/count(distinct date) as new_seller_clicks_per_day,
	sum(case when b.shop_id is not null then budget end)/sum(budget) as new_seller_budget_share,
	sum(case when b.shop_id is not null then spend end)/sum(spend) as new_seller_spend_share,
	sum(case when b.shop_id is not null then impression_count end)/sum(impression_count) as new_seller_imp_share,
	sum(case when b.shop_id is not null then click_count end)/sum(click_count) as new_seller_click_share
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a left join
	base b on a.date = b.first_prolist_date and a.shop_id = b.shop_id
where
	date >= "2016-01-01"
group by 1
order by 1
;


-- outcome:
-- what would happen if we replaced impressions in the current hour with the constrained sellers
-- for a seller who has a last impression hour of 12
with base as (
select
	shop_id,
	max(impression_hour) as last_impression_hour	
from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps`
where
	budget_util = 1
group by 1
),base2 as (
select
	last_impression_hour,
	sum(click) as constrained_clicks,
	count(*) as constrained_imps
from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps` a left join
	base b on a.shop_id = b.shop_id
where
	budget_util = 1
group by 1
),base3 as (
select
	impression_hour,
	sum(click) as total_clicks,
	count(*) as total_impressions
from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps`
group by 1
),base4 as (
select
	a.impression_hour,
	b.last_impression_hour,
	a.total_clicks,
	a.total_impressions,
	b.constrained_clicks,
	b.constrained_imps,
	row_number() over(partition by impression_hour) as rn
from
	base3 a cross join
	base2 b
order by 1,2
)
select
	impression_hour,
	a.total_clicks/a.total_impressions as real_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour = impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour = impression_hour - 1 then constrained_imps end)) as hour_1_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 2 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 2 and impression_hour - 1 then constrained_imps end)) as hour_2_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 3 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 3 and impression_hour - 1 then constrained_imps end)) as hour_3_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 4 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 4 and impression_hour - 1 then constrained_imps end)) as hour_4_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 5 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 5 and impression_hour - 1 then constrained_imps end)) as hour_5_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 6 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 6 and impression_hour - 1 then constrained_imps end)) as hour_6_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 7 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 7 and impression_hour - 1 then constrained_imps end)) as hour_7_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 8 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 8 and impression_hour - 1 then constrained_imps end)) as hour_8_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 9 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 9 and impression_hour - 1 then constrained_imps end)) as hour_9_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 10 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 10 and impression_hour - 1 then constrained_imps end)) as hour_10_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 11 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 11 and impression_hour - 1 then constrained_imps end)) as hour_11_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 12 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 12 and impression_hour - 1 then constrained_imps end)) as hour_12_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 13 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 13 and impression_hour - 1 then constrained_imps end)) as hour_13_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 14 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 14 and impression_hour - 1 then constrained_imps end)) as hour_14_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 15 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 15 and impression_hour - 1 then constrained_imps end)) as hour_15_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 16 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 16 and impression_hour - 1 then constrained_imps end)) as hour_16_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 17 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 17 and impression_hour - 1 then constrained_imps end)) as hour_17_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 18 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 18 and impression_hour - 1 then constrained_imps end)) as hour_18_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 19 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 19 and impression_hour - 1 then constrained_imps end)) as hour_19_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 20 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 20 and impression_hour - 1 then constrained_imps end)) as hour_20_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 21 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 21 and impression_hour - 1 then constrained_imps end)) as hour_21_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 22 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 22 and impression_hour - 1 then constrained_imps end)) as hour_22_ctr,
	(sum(case when rn = 1 then a.total_clicks end)+sum(case when last_impression_hour between impression_hour - 23 and impression_hour - 1 then constrained_clicks end))/(sum(case when rn = 1 then a.total_impressions end)+sum(case when last_impression_hour between impression_hour - 23 and impression_hour - 1 then constrained_imps end)) as hour_23_ctr
from
	base4 a
group by 1,2
order by 1
;


select
	a.impression_hour,
	a.total_clicks/a.total_impressions as real_ctr,
	(sum(a.total_clicks)+sum(case when last_impression_hour = impression_hour - 1 then constrained_clicks end))/(sum(a.total_impressions)+sum(case when last_impression_hour = impression_hour - 1 then constrained_imps end)) as hour_1_ctr,
	(sum(a.total_clicks)+sum(case when last_impression_hour <= impression_hour - 2 then constrained_clicks end))/(sum(a.total_impressions)+sum(case when last_impression_hour <= impression_hour - 2 then constrained_imps end)) as hour_2_ctr,
	(sum(a.total_clicks)+sum(case when last_impression_hour <= impression_hour - 3 then constrained_clicks end))/(sum(a.total_impressions)+sum(case when last_impression_hour <= impression_hour - 3 then constrained_imps end)) as hour_3_ctr,
	(sum(a.total_clicks)+sum(case when last_impression_hour <= impression_hour - 4 then constrained_clicks end))/(sum(a.total_impressions)+sum(case when last_impression_hour <= impression_hour - 4 then constrained_imps end)) as hour_4_ctr
from
	base3 a cross join
	base2 b
group by 1,2
order by 1
;

-- what percent of queries without inventory have organic inventory?
with base as (
select
	distinct
	a.visit_id,
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "listing_id") as prolist_listings
from
	`etsy-visit-pipe-prod.canonical.visits_recent` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_ranking_signals"
where
	date(_PARTITIONTIME) BETWEEN CURRENT_DATE - 3 AND CURRENT_DATE - 1 
),base2 as (
select
	a.visit_id,
	a.query,
	prolist_listings,
	sum(impressions) as total_impressions
from
	base a left join
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions` b on a.visit_id = b.visit_id and a.query = b.query and b.page_no = 1
where
	prolist_listings = "[]" and _date between current_date - 3 and current_date - 1
group by 1,2,3
)
select
	count(case when total_impressions > 0 then visit_id end)/count(*) as share_with_organic,
	count(case when total_impressions >= 16 then visit_id end)/count(*) as share_with_page_1
from
	base2
;

	last_impression_hour,
	sum(a.click) as real_clicks,
	count(*)
	sum(click)/count(*) as ctr
from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps` a join
	base2 b on a.impression_hour = b.last_impression_hour + 1
	)

with base as (
select
	shop_id,
	clicks,
	imps,
	spend,
	budget,
	revenue,
	max(impression_hour)+1 as last_hour	
from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps`
where
	budget_util = 1
group by 1,2,3,4,5,6
),base2 as (
select
	impression_hour+1 as imp_hour,
	count(*) as total_imps,
	sum(a.click)/count(*) as ctr,
	sum(case when click = 1 then cost end)/sum(click) as cpc,
	sum(clicks)/sum(sum(clicks)) over() as click_share,
	count(distinct shop_id) as shop_count,

from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps` a left join

group by 1
)

select
	last_hour,
	sum(spend)/sum(sum(spend)) over() as spend_share,
	sum(imps)/sum(sum(imps)) over() as imp_share,
	sum(imps)/sum(last_hour) as imps_per_hour,
	sum(clicks)/sum(imps) as ctr,
	sum(budget)/count(distinct shop_id) as budget_per_shop,
	sum(revenue)/sum(spend) as ROAS
from
	base
group by 1
order by 1
;

-- what is the average CTR for constrained and non-constrained impressions
with visit_base as (
select
	query_visit_session,
	avg(case when budget_util < 1 then predctr end) as pred_ctr_unconstrained,
	avg(case when budget_util = 1 then predctr end) pred_ctr_constrained,
	count(case when budget_util = 1 then query end) as constrained_imps,
	count(case when budget_util < 1 then query end) as unconstrained_imps
from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps`
where
	rank between 1 and 20
group by 1
),base2 as (
select
	a.shop_id,
	a.query_visit_session,
	budget,
	spend,
	pred_ctr_unconstrained,
	pred_ctr_constrained,
	constrained_imps,
	unconstrained_imps,
	avg(predctr) as predctr,
	max(impression_hour) as impression_hour
from
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps` a join
	visit_base b on a.query_visit_session = b.query_visit_session
where
	budget_util = 1 and rank between 1 and 20
group by 1,2,3,4,5,6,7,8
),base as (
select
	shop_id,
	sum(predctr) as total_predctr,
	count(*) as total_impressions,
	max(impression_hour) as last_imp_hour,
	sum(budget) as total_budget,
	sum(pred_ctr_unconstrained) as total_unconstrained_pred_ctr,
	sum(pred_ctr_constrained) as total_constrained_pred_ctr,
	sum(unconstrained_imps) as unconstrained_imps,
	sum(constrained_imps) as constrained_imps
	-- sum(constrained_imps)/(sum(constrained_imps)+sum(unconstrained_imps)) as constrained_imp_share
from
	base2
group by 1
)
select
	last_imp_hour,
	count(distinct shop_id) as shop_count,
	count(distinct shop_id)/sum(count(distinct shop_id)) over() as shop_share,
	avg(total_budget) as avg_budget,
	sum(total_predctr)/sum(total_impressions) as predctr,
	sum(total_constrained_pred_ctr)/sum(constrained_imps) as constrained_ctr,
	sum(total_unconstrained_pred_ctr)/sum(unconstrained_imps) as unconstrained_ctr,
	sum(constrained_imps)/(sum(constrained_imps)+sum(unconstrained_imps)) as constrained_imp_share
from
	base
group by 1
order by 1
;

-- has the impression gap closed as we've added more budget?
with base as (
SELECT
	_DATE as date,
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "listing_id") as listings
FROM
`etsy-visit-pipe-prod.canonical.visits_sampled` a
INNER JOIN
UNNEST(a.events.events_tuple) AS b ON b.event_type = "prolist_ranking_signals"
WHERE
	_date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1 
)
select
	date, 
	count(*) as ad_requests,
	count(case when listings="[]" then query end) as empty_ad_requests,
	count(case when listings="[]" then query end)/count(*) as share_empty_requests
from base
group by 1
order by 1
;

-- budget constrained sellers
select
	date_trunc(date,week) as week,
	count(distinct case when spend/budget >= 0.9 then shop_id end)/count(distinct shop_id) as budget_constrained_shop,
	sum(case when spend/budget = 1 then spend end)/sum(spend) as budget_constrained_spend	
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
where
	date = "2021-11-08" and impressions_last_4w > 0
;

-- budget constraints over time
with base as (
select
	date_trunc(date,month) as month,
	count(distinct case when spend/budget >= 0.9 then shop_id end)/count(distinct shop_id) as budget_constrained_shops,
	sum(case when spend/budget = 1 then spend end)/sum(spend) as budget_constrained_spend
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` 
where
	date >= "2016-01-01"
group by 1
order by 1
)
select
	a.*,
	sum(b.etsy_visits)/sum(b.ly_etsy_visits) - 1 as etsy_visits_yy,
	sum(b.adjusted_budget)/sum(b.ly_adjusted_budget) - 1 as adj_budget_yy
from
	base a join
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary` b on a.month = date_trunc(date,month)
group by 1,2,3
order by 1
;


