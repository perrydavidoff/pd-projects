-- Etsy Ads long term strategy opportunity buckets
-- Date: November, 2021
-- Overview: this part of the analysis looks into revenue opportunities available over the next few years
-- and to define what the TAM is for Etsy ads

select
	date_trunc(a.date,month) as month,
	sum(revenue) as total_revenue,
	sum(impressions) as total_impressions,
	(sum(clicks)/sum(impressions)) as ctr,
	(sum(spend)/sum(clicks)) as cpc,
	sum(converting_clicks)/sum(clicks) as pccvr,
	(sum(revenue)/sum(spend)) as roas,
	sum(revenue)/sum(ly_revenue)-1 as revenue_yy,
	sum(impressions)/sum(ly_impressions)-1 as imp_yy,
	(sum(clicks)/sum(impressions))/(sum(ly_clicks)/sum(ly_impressions))-1 as ctr_yy,
	(sum(spend)/sum(clicks))/(sum(ly_spend)/sum(ly_clicks))-1 as cpc_yy,
	(sum(converting_clicks)/sum(clicks))/(sum(ly_converting_clicks)/sum(ly_clicks))-1 as pccvr_yy,
	(sum(revenue)/sum(spend))/(sum(ly_revenue)/sum(ly_spend))-1 as roas_yy,
	sum(etsy_visits)/sum(ly_etsy_visits)-1 as etsy_visits_yy,
	sum(budget)/sum(ly_budget)-1 as budget_yy,
	sum(adjusted_budget)/sum(ly_adjusted_budget)-1 as adjusted_budget_yy,
	sum(num_sellers_impressions_l4w)/sum(active_seller_ct) as adopted_sellers_l4w,
	sum(num_sellers_clicks_l4w)/sum(active_seller_ct) as click_sellers_l4w,
	(sum(spend)/sum(adjusted_budget)) as budget_util,
	(sum(spend)/sum(adjusted_budget))/(sum(ly_spend)/sum(ly_adjusted_budget))-1 as budget_util_yy
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary` a left join
	`etsy-data-warehouse-prod.rollups.active_sellers_rollup_daily_12m` b on a.date = b.date
group by 1
order by 1
;

-- how much revenue have we banked with experiments by year?

select
	-- a.launch_date,
	start_date,
	end_date,
	extract(year from end_date) as end_year,
	a.initiative,
	a.subteam,
	a.experiment_name,
	-- a.start_date,
	-- a.end_date,
	a.rev_coverage,
	a.prolist_pct_change,
	-- a.gms_coverage,
	-- a.rev_ann,
	layer_start,
	layer_end,
	status
	-- a.launch_id
from
	`etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` a left join
	`etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b on a.experiment_name = b.name
where
	subteam in ("Buyer Ads Experience","Ads","Ad Ranking","Prolist",
		"Seller Ads and Insights") 
	-- and prolist_pct_change > 0 and 
	-- status like "%Ramped Up%"
order by end_date
;

-- share of imps from diff pages
select
	page_type,
	count(*)/sum(count(*)) over() as imp_share
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	_PARTITIONDATE >= "2021-10-28"
group by 1
order by 2 desc
;

select
	platform,
	count(*)/sum(count(*)) over() as imp_share 
from
	`etsy-data-warehouse-prod.weblog.events` a join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date >= "2021-10-25"
where
	platform in ("boe","desktop","mobile_web") and a.event_type = "prolist_imp_full"
	and timestamp_seconds(a.run_date) >= "2021-10-25"
group by 1
order by 2 desc
;





`etsy-data-warehouse-dev.pdavidoff.`

`etsy-data-warehouse-dev.mdelgado.daily_summary_segments_temp`
-- daily impression count, and revenue per impression

with base as (
select
	_date,
	sum(impressions) as total_impressions,
	sum(spend) as total_spend,
	sum(spend)/sum(impressions) as spend_per_imp
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary`
where
	event_type = "prolist_imp_full"
group by 1
order by 1
)
select
	avg(impression_count) as avg_impressions
from
	base
;

-- closing the hourly impression gap
with imps as (
select
	_date as date,
    EXTRACT(hour FROM timestamp_millis(epoch_ms) AT TIME ZONE "America/New_York") as hour,
	count(*) as impression_count
from
	`etsy-data-warehouse-prod.weblog.events`
where
	event_type = "prolist_imp_full" and _date > "2021-10-01"
group by 1,2
),agg_imp as (
select
	hour,
	avg(impression_count) as avg_impr_count
from
	imps
group by 1
),total_clicks as (
select
	extract(date from timestamp_seconds(click_date) AT TIME ZONE "America/New_York") as date,
	extract(hour from timestamp_seconds(click_date) AT TIME ZONE "America/New_York") as hour,
	count(*) as clicks,
	sum(cost)/100 as total_spend
from
	`etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
where
	extract(date from timestamp_seconds(click_date) AT TIME ZONE "America/New_York") > "2021-10-01"
group by 1,2
),agg_clicks as (
select
	hour,
	avg(clicks) as avg_clicks,
	avg(total_spend) as avg_spend
from
	total_clicks
group by 1
)
select
	a.hour,
	a.avg_impr_count,
	b.avg_clicks,
	b.avg_spend,
	b.avg_spend/a.avg_impr_count as spend_per_imp
from
	agg_imp a join
	agg_clicks b on a.hour = b.hour
order by 1
;


-- increase inventory. how much revenue coverage on different platforms?
select
	platform,
	sum(cost)/sum(sum(cost)) over() as spend_share	
from
	`etsy-data-warehouse-prod.rollups.prolist_click_mart`
where
	timestamp_seconds(run_date) > "2021-10-01"
group by 1
order by 2 desc
;

-- what is activity on the listing page?
with base as (
select
	distinct
	visit_id,
	1 as listing_view
from
	`etsy-data-warehouse-prod.weblog.events`
where
	_date >= "2021-10-16" and event_type = "view_listing"
),att_imps as (
select
	a.visit_id,
	max(case when b.visit_id is not null then 1 else 0 end) as imp,
	count(case when b.visit_id is not null then logging_key end) as imps_per_visit,
	count(case when b.click = 1 then logging_key end) as clicks
from
	base a left join
	`etsy-prolist-etl-prod.prolist.attributed_impressions` b on a.visit_id = b.visit_id and page_type = 8 and _PARTITIONDATE >= "2021-10-16"
group by 1
)
select
	count(distinct visit_id) as visit_count,
	count(distinct case when imp = 1 then visit_id end)/count(distinct visit_id) as visits_with_imp,
	sum(imps_per_visit)/count(distinct case when imp = 1 then visit_id end) as imps_per_visit,
	sum(clicks)/sum(imps_per_visit) as ctr
from
	att_imps
;

-- past year revenue
select
	sum(spend)
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
where
	date_diff(current_date(),date,day) <= 365
;

select
	page_type,
	sum(cost) as cost
	-- count(distinct extract(date from timestamp_seconds(cast(timestamp as int64)))) as dates
from
	`etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
where
	date_diff(current_date(),extract(date from timestamp_seconds(click_date)),day) <= 365
group by 1
order by 2 desc;
-- email channel for EA
select
	event_referrer_type,
	ref_tag,
	count(*)
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date = "2021-10-18" and referring_page_event is null
group by 1,2
order by 3 desc
;

-- use listing views instead
with base as (
select
	distinct
	visit_id,
	converted,
	total_gms,
	utm_campaign,
	top_channel
from
	`etsy-data-warehouse-prod.weblog.recent_visits`
where
	_date = "2021-10-18" and platform in ("desktop","mobile_web")
)
select
	-- visit_id,
	case when is_first_page = 1 and referring_page_event is null then top_channel else referring_page_event end as referring_page_event,
	-- visits with that referring event over total visits for date
	count(distinct b.visit_id)/25316251 as visit_count,
	count(listing_id)/count(distinct b.visit_id) as listings_per_visit,
	avg(converted) as purchased_visit,
	avg(purchased_after_view) as purchase_after_view_rate
from
	base a left join
	`etsy-data-warehouse-prod.analytics.listing_views` b on a.visit_id = b.visit_id and b._date = "2021-10-18" and platform in ("desktop","mobile_web")
group by 1
order by 2 desc
;

select
	top_chanmnel
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	is_first_page = 1 and _date = "2021-10-18"
limit 50
;
from
	`etsy-data-warehouse-prod.weblog.recent_visits`
select
	count(distinct visit_id) as visit_count
from
	`etsy-data-warehouse-prod.weblog.visits`
where
	platform in ("desktop","mobile_web") and 	date_diff(current_date(),_date,day) <= 365
	and _date >= "2020-10-18"
;

select
	page_type,
	sum(cost)/count(*) as cpc,
	sum(cost)/100 as total_cost
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	date_diff(current_date(),_PARTITIONDATE,day) <= 365 and click = 1
group by 1
order by 2 desc
;
-- recommendations share seen on cart page
with base as (
select
	visit_id
from
	`etsy-data-warehouse-prod.weblog.recent_visits`
where
	_date >= "2021-10-12" and platform in ("desktop","mobile_web")
),join_events as (
select
	a.visit_id,
	max(case when event_type = "recommendations_module_seen" then 1 else 0 end) as module_seen_flag,
	max(case when event_type = "recommendations_module_delivered" then 1 else 0 end) as module_delivered_flag,
	max(case when event_type = "cart_recs_clickT" then 1 else 0 end) as module_click_flag,
	count(case when event_type = "recommendations_module_seen" then event_type end) as module_seen_event_count,
	count(case when event_type = "recommendations_module_delivered" then a.visit_id end) as module_delivered_count
from
	base a left join
	`etsy-data-warehouse-prod.weblog.events` b on a.visit_id = b.visit_id and b.event_type in ("recommendations_module_seen","recommendations_module_delivered","cart_recs_clickT") and _date >= "2021-10-12" and url like "%/cart/%"

group by 1
)
select
	count(distinct visit_id) as visit_count,
	count(distinct case when module_seen_flag = 1 then visit_id end)/count(distinct visit_id) as module_seen_rate,
	count(distinct case when module_delivered_flag = 1 then visit_id end)/count(distinct visit_id) as module_delivered_rate,
	count(distinct case when module_click_flag = 1 then visit_id end)/count(distinct visit_id) as module_click_rate
from
	join_events
;

select
	referring_page_event,
	count(*)/sum(count(*)) over() as view_share	
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date = "2021-10-18" and platform in ("desktop","mobile_web")
group by 1
order by 2 desc
;

select
	page_type,
	sum(click)/sum(sum(click)) over() as click_share,
	sum(click)/count(*) as ctr,
	sum(cost)/sum(click) as cpc
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	page_type in (0,1,8) and _PARTITIONDATE >= "2021-10-12"
group by 1
order by 2 desc
;

select
	min(_date)
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date >= "2014-01-01"
;

-- query visit sessions
with base as (
select
	a._date as date,
	a.visit_id||a.query as visit_query_id,
	max(case when b.visit_id is not null then 1 else 0 end) as prolist_count
from
	`etsy-data-warehouse-prod.search.query_sessions_new` a left join
	`etsy-prolist-etl-prod.prolist.attributed_impressions` b on a.visit_id||a.query = b.visit_id||b.query and b._PARTITIONDATE >= "2021-09-01"
where
	_date >= "2021-09-01"
group by 1,2
)
select
	date,
	count(distinct case when prolist_count = 0 then visit_query_id end)/count(distinct visit_query_id) as no_imp_prolist
from
	base
group by 1
order by 1
;


-- share of clicks for prolist
with prolist_clicks as (
select
	date_trunc(date,month) as month,
	sum(clicks) as total_clicks
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary`
where
	date >= "2016-01-01"
group by 1
),total_listing_views as (
select
	date_trunc(_date,month) as listing_view_date,
	count(*) as listing_views
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date >= "2016-01-01"
group by 1
)
select
	a.month,
	a.total_clicks as prolist_clicks,
	b.listing_views as total_listing_views,
	a.total_clicks/b.listing_views as prolist_click_share	
from
	prolist_clicks a join
	total_listing_views b on a.month = b.listing_view_date
order by 1
;

-- search vs. prolist ctr depreciation
select
	distinct
	page
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions`
where
	_date >= "2021-11-08"
;

with ad_impressions as (
select 
	extract(hour from timestamp_seconds(cast(a.timestamp as INT64)) at TIME ZONE "America/New_York") as hour,
	count(*) as impressions,
	count(case when click = 1 then logging_key end) as clicks
from 
	`etsy-prolist-etl-prod.prolist.attributed_impressions` a
where 
	_PARTITIONDATE between "2021-10-09" and "2021-10-14" and page_type in (0,1,2)
group by 1
),organic as (
select 
	extract(hour from b.start_datetime at TIME ZONE "America/New_York") as hour,
	sum(a.impressions) as impressions,
	sum(a.clicks) as clicks
from 
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions` a join 
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id
	where
		a._date between "2021-10-09" and "2021-10-14" and b._date between "2021-10-09" and "2021-10-14"
	group by 1
	)
	select
		a.hour,
		a.impressions as ad_impressions,
		a.clicks as ad_clicks,
		a.clicks/a.impressions as ad_ctr,
		c.impressions as organic_impressions,
		c.clicks as organic_clicks,
		c.clicks/c.impressions as organic_ctr
from 
	ad_impressions a left join 
	organic c on a.hour=c.hour
order by 1
;


-- what happened to ctr depreciation when we had 2:1 on?
with experiment_visits as (
select
	visit_id
from
	`etsy-data-warehouse-prod.catapult.ab_tests`
where
	ab_test in ("ranking/badx.2021_q3.inventory_expansion.boe_v2","ranking/badx.2021_q3.inventory_expansion.desktop",
		"ranking/badx.2021_q3.inventory_expansion.mweb") and ab_variant = "on" 
	and _date between "2021-09-25" and "2021-09-30"
),ad_impressions as (
select 
	extract(hour from timestamp_seconds(cast(timestamp as INT64)) at TIME ZONE "America/New_York") as hour,
	count(*) as impressions,
	count(case when click = 1 then logging_key end) as clicks
from 
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where 
	visit_id in (select visit_id from experiment_visits) and _PARTITIONDATE between "2021-09-25" and "2021-09-30" 
	and page_type in (0,1,2)
group by 1
),organic as (
select 
	extract(hour from b.start_datetime at TIME ZONE "America/New_York") as hour,
	sum(a.impressions) as impressions,
	sum(a.clicks) as clicks
from 
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions` a join 
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id
	where
		a.visit_id in (select visit_id from experiment_visits) and
		a._date between "2021-09-25" and "2021-09-30" and 
		b._date between "2021-09-25" and "2021-09-30"
	group by 1
	)
	select
		a.hour,
		a.impressions as ad_impressions,
		a.clicks as ad_clicks,
		a.clicks/a.impressions as ad_ctr,
		c.impressions as organic_impressions,
		c.clicks as organic_clicks,
		c.clicks/c.impressions as organic_ctr
from 
	ad_impressions a left join 
	organic c on a.hour=c.hour
order by 1
;


