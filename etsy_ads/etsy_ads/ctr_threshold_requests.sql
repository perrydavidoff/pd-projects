-- title: Extreme high PCTR Thresholds
-- date: 12/7/2021
-- goal: pull predicted ctr from recent extreme high experiments to see whether there's a threshold that can
-- be launched that doesn't have a negative impact on CR
-- findings: after looking at both mweb and desktop (and using pred CTR and pred CVR as dimensions) there wasn't a
-- clear cutoff that has a neutral impact on CR.

-- ctr threshold requests
create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.ea_prs_threshold_exp`
	as (
select
	distinct
	_date as date,
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "listing_id") as prolist_listings,
	(select value from unnest(properties.map) where key = "predCtr") as pred_ctr,
	(select value from unnest(properties.map) where key = "predCvr") as pred_cvr,
	(select value from unnest(properties.map) where key = "page_type") as page_type,
	(select value from unnest(properties.map) where key = "guid") as guid,
	(select value from unnest(properties.map) where key = "page_guid") as page_guid,
	(select value from unnest(properties.map) where key = "ref") as ref,
	(select value from unnest(properties.map) where key = "loc") as loc
from
	`etsy-visit-pipe-prod.canonical.visits` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_ranking_signals"
where
	_date BETWEEN "2021-10-19" and "2021-10-25"
)
;

-- prolist click full requests
create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.ea_click_requests_threshold_exp`
	as (
select
	distinct
	_date as date,
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "listing_id") as listing_id,
	(select value from unnest(properties.map) where key = "predCtr") as pred_ctr,
	(select value from unnest(properties.map) where key = "nonce") as nonce,
	(select value from unnest(properties.map) where key = "page_type") as page_type,
	(select value from unnest(properties.map) where key = "guid") as guid,
	(select value from unnest(properties.map) where key = "page_guid") as page_guid,
	(select value from unnest(properties.map) where key = "cost") as cost,
	(select value from unnest(properties.map) where key = "ref") as ref,
	(select value from unnest(properties.map) where key = "loc") as loc
from
	`etsy-visit-pipe-prod.canonical.visits` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_click_full"
where
	_date BETWEEN "2021-10-19" and "2021-10-25"
)
;

select
	nonce
from
	`etsy-data-warehouse-dev.pdavidoff.ea_click_requests_threshold_exp`
limit 50;

with base as (
select
	visit_id,
	page_guid,
	count(*) as query_count	
from
	`etsy-data-warehouse-dev.pdavidoff.ea_prs_threshold_exp`
where
	page_type = "1"
group by 1,2
)
select
	count(case when query_count >1 then visit_id end)/count(visit_id)
from
	base
;

select
	date,
	visit_id,
	event_time,
	query,
	row_number() over(order by event_time) as query_no,
	lead(event_time) over(partition by visit_id,query order by event_time) as next_event_time
from
	`etsy-data-warehouse-dev.pdavidoff.ea_prs_threshold_exp`
where
	page_type = "1"
limit 50;

create or replace table
  `etsy-data-warehouse-dev.pdavidoff.ctr_threshold_unnest`
  as (
with query_rank as (
select
	date,
	visit_id,
	event_time,
	query,
	prolist_listings,
	page_guid,
	guid,
	pred_ctr,
	pred_cvr,
	page_type,
	row_number() over(order by event_time) as query_no,
	lead(event_time) over(partition by visit_id,query order by event_time) as next_event_time
from
	`etsy-data-warehouse-dev.pdavidoff.ea_prs_threshold_exp`
where
	page_type = "1"
),base2 as (
select
	visit_id,
	date,
	event_time,
	query,
	page_type,
	query_no,
	case when next_event_time is null then "2021-10-26" else next_event_time end as next_event_time,
	safe_cast(replace(replace(listing_id,"[",""),"]","") as int64) as listing_id,
	safe_cast(replace(replace(pred_ctr,"[",""),"]","") as float64) as pred_ctr,
	safe_cast(replace(replace(pred_cvr,"[",""),"]","") as float64) as pred_cvr,
	row_number() over(partition by query_no) as listing_rank
from
	query_rank, 
	unnest(split(prolist_listings)) as listing_id with offset pos1, 
	unnest(split(pred_ctr)) as pred_ctr with offset pos2,
	unnest(split(pred_cvr)) as pred_cvr with offset pos3
where
	(pos1 = pos2 and pos2 = pos3 and pos1 = pos3)
),request as (
select
	visit_id,
	date,
	query,
	query_no,
	event_time,
	next_event_time,
	avg(pred_ctr) as avg_pred_ctr,
	avg(pred_cvr) as avg_pred_cvr,
	max(listing_rank) as max_listing,
	min(listing_rank) as min_listing,
	count(*) as impression_count
from
	base2
where
	listing_id is not null and pred_ctr is not null and pred_cvr is not null
group by 1,2,3,4,5,6
),join_clicks as (
select
	a.*,
	count(b.visit_id) as click_count,
	sum(cost) as total_spend
from
	request a left join
	`etsy-data-warehouse-prod.rollups.prolist_click_mart` b on a.visit_id = b.visit_id and a.query = b.query and b.click_timestamp between a.event_time and a.next_event_time
group by 1,2,3,4,5,6,7,8,9,10,11
),visit_gms as (
select
	a.*,
	b.converted,
	b.total_gms	
from
	join_clicks a join
	`etsy-data-warehouse-prod.weblog.visits` b on a.visit_id = b.visit_id and b._date between "2021-10-18" and "2021-10-26"
)
select
	a.*,
	case when b.ab_variant is not null then b.ab_variant else null end as mweb_variant,
	case when c.ab_variant is not null then c.ab_variant else null end as desktop_variant
from
	visit_gms a left join
	`etsy-data-warehouse-prod.catapult.ab_tests` b on a.visit_id = b.visit_id and b.ab_test = "ranking/badx.2021_q3.pred_ctr_based_layout.market.mweb" and b._date between "2021-10-19" and "2021-10-25" left join
	`etsy-data-warehouse-prod.catapult.ab_tests` c on a.visit_id = c.visit_id and c.ab_test = "ranking/badx.2021_q3.pred_ctr_based_layout.market.desktop" and c._date between "2021-10-19" and "2021-10-25"
)
;

-- let's look at pred ctr first
-- mweb results first (pred_ctr)
with base as (
select
	*,
	ntile(10) over(order by avg_pred_ctr) as ctr_decile
from
	(select * from `etsy-data-warehouse-dev.pdavidoff.ctr_threshold_unnest` where avg_pred_ctr >= 0.015 and mweb_variant is not null)
)
select 
	-- mweb_variant,
	ctr_decile,
	min(avg_pred_ctr) as min_ctr,
	max(avg_pred_ctr) as max_ctr,
	count(case when mweb_variant = "on" then visit_id end) as on_ad_request_count,
	count(case when mweb_variant = "off" then visit_id end) as off_ad_request_count,
	sum(case when mweb_variant = "on" then click_count end) as on_total_clicks,
	sum(case when mweb_variant = "off" then click_count end) as off_total_clicks,
	count(case when mweb_variant = "on" and converted = 1 then visit_id end) as on_total_purch,
	count(case when mweb_variant = "off" and converted = 1 then visit_id end) as off_total_purch,
	sum(case when mweb_variant = "on" then impression_count end)/sum(case when mweb_variant = "off" then impression_count end)-1 as result_pct_change,
	(count(case when mweb_variant = "on" and click_count >= 1 then visit_id end)/count(case when mweb_variant = "on" then visit_id end))/(count(case when mweb_variant = "off" and click_count >= 1 then visit_id end)/count(case when mweb_variant = "off" then impression_count end))-1 as click_rate_pct_change,
	(sum(case when mweb_variant = "on" then total_spend end)/count(case when mweb_variant = "on" then visit_id end))/(sum(case when mweb_variant = "off" then total_spend end)/count(case when mweb_variant = "off" then visit_id end))-1 as spend_pct_change,
	(count(case when mweb_variant = "on" and converted = 1 then visit_id end)/count(case when mweb_variant = "on" then visit_id end))/(count(case when mweb_variant = "off" and converted = 1 then visit_id end)/count(case when mweb_variant = "off" then visit_id end))-1 as visit_cr_change,
	(sum(case when mweb_variant = "on" then total_spend+(total_gms*0.14) end)/count(case when mweb_variant = "on" then visit_id end))/(sum(case when mweb_variant = "off" then total_spend+(total_gms*0.14) end)/count(case when mweb_variant = "off" then visit_id end))-1 as total_revenue_chg
from
	base
group by 1
order by 1
;

-- desktop version for pred CTR
with base as (
select
	*,
	ntile(10) over(order by avg_pred_ctr) as ctr_decile
from
	(select * from `etsy-data-warehouse-dev.pdavidoff.ctr_threshold_unnest` where avg_pred_ctr >= 0.015 and desktop_variant is not null)
)
select 
	-- mweb_variant,
	ctr_decile,
	min(avg_pred_ctr) as min_ctr,
	max(avg_pred_ctr) as max_ctr,
	count(case when desktop_variant = "on" then visit_id end) as on_ad_request_count,
	count(case when desktop_variant = "off" then visit_id end) as off_ad_request_count,
	sum(case when desktop_variant = "on" then click_count end) as on_total_clicks,
	sum(case when desktop_variant = "off" then click_count end) as off_total_clicks,
	count(case when desktop_variant = "on" and converted = 1 then visit_id end) as on_total_purch,
	count(case when desktop_variant = "off" and converted = 1 then visit_id end) as off_total_purch,
	sum(case when desktop_variant = "on" then impression_count end)/sum(case when desktop_variant = "off" then impression_count end)-1 as result_pct_change,
	(count(case when desktop_variant = "on" and click_count >= 1 then visit_id end)/count(case when desktop_variant = "on" then visit_id end))/(count(case when desktop_variant = "off" and click_count >= 1 then visit_id end)/count(case when desktop_variant = "off" then impression_count end))-1 as click_rate_pct_change,
	(sum(case when desktop_variant = "on" then total_spend end)/count(case when desktop_variant = "on" then visit_id end))/(sum(case when desktop_variant = "off" then total_spend end)/count(case when desktop_variant = "off" then visit_id end))-1 as spend_pct_change,
	(count(case when desktop_variant = "on" and converted = 1 then visit_id end)/count(case when desktop_variant = "on" then visit_id end))/(count(case when desktop_variant = "off" and converted = 1 then visit_id end)/count(case when desktop_variant = "off" then visit_id end))-1 as visit_cr_change,
	(sum(case when desktop_variant = "on" then total_spend+(total_gms*0.14) end)/count(case when desktop_variant = "on" then visit_id end))/(sum(case when desktop_variant = "off" then total_spend+(total_gms*0.14) end)/count(case when desktop_variant = "off" then visit_id end))-1 as total_revenue_chg
from
	base
group by 1
order by 1
;


-- predicted cvr now
-- first, mweb
with base as (
select
	*,
	ntile(10) over(order by avg_pred_cvr) as cvr_decile
from
	(select * from `etsy-data-warehouse-dev.pdavidoff.ctr_threshold_unnest` where avg_pred_ctr >= 0.015 and mweb_variant is not null)
)
select 
	-- mweb_variant,
	cvr_decile,
	min(avg_pred_cvr) as min_ctr,
	max(avg_pred_cvr) as max_ctr,
	count(case when mweb_variant = "on" then visit_id end) as on_ad_request_count,
	count(case when mweb_variant = "off" then visit_id end) as off_ad_request_count,
	sum(case when mweb_variant = "on" then click_count end) as on_total_clicks,
	sum(case when mweb_variant = "off" then click_count end) as off_total_clicks,
	count(case when mweb_variant = "on" and converted = 1 then visit_id end) as on_total_purch,
	count(case when mweb_variant = "off" and converted = 1 then visit_id end) as off_total_purch,
	sum(case when mweb_variant = "on" then impression_count end)/sum(case when mweb_variant = "off" then impression_count end)-1 as result_pct_change,
	(count(case when mweb_variant = "on" and click_count >= 1 then visit_id end)/count(case when mweb_variant = "on" then visit_id end))/(count(case when mweb_variant = "off" and click_count >= 1 then visit_id end)/count(case when mweb_variant = "off" then impression_count end))-1 as click_rate_pct_change,
	(sum(case when mweb_variant = "on" then total_spend end)/count(case when mweb_variant = "on" then visit_id end))/(sum(case when mweb_variant = "off" then total_spend end)/count(case when mweb_variant = "off" then visit_id end))-1 as spend_pct_change,
	(count(case when mweb_variant = "on" and converted = 1 then visit_id end)/count(case when mweb_variant = "on" then visit_id end))/(count(case when mweb_variant = "off" and converted = 1 then visit_id end)/count(case when mweb_variant = "off" then visit_id end))-1 as visit_cr_change,
	(sum(case when mweb_variant = "on" then total_spend+(total_gms*0.14) end)/count(case when mweb_variant = "on" then visit_id end))/(sum(case when mweb_variant = "off" then total_spend+(total_gms*0.14) end)/count(case when mweb_variant = "off" then visit_id end))-1 as total_revenue_chg
from
	base
group by 1
order by 1
;


-- then, desktop
-- desktop version for pred CTR
with base as (
select
	*,
	ntile(10) over(order by avg_pred_cvr) as cvr_decile
from
	(select * from `etsy-data-warehouse-dev.pdavidoff.ctr_threshold_unnest` where avg_pred_ctr >= 0.015 and desktop_variant is not null)
)
select 
	-- mweb_variant,
	cvr_decile,
	min(avg_pred_cvr) as min_cvr,
	max(avg_pred_cvr) as max_cvr,
	count(case when desktop_variant = "on" then visit_id end) as on_ad_request_count,
	count(case when desktop_variant = "off" then visit_id end) as off_ad_request_count,
	sum(case when desktop_variant = "on" then click_count end) as on_total_clicks,
	sum(case when desktop_variant = "off" then click_count end) as off_total_clicks,
	count(case when desktop_variant = "on" and converted = 1 then visit_id end) as on_total_purch,
	count(case when desktop_variant = "off" and converted = 1 then visit_id end) as off_total_purch,
	sum(case when desktop_variant = "on" then impression_count end)/sum(case when desktop_variant = "off" then impression_count end)-1 as result_pct_change,
	(count(case when desktop_variant = "on" and click_count >= 1 then visit_id end)/count(case when desktop_variant = "on" then visit_id end))/(count(case when desktop_variant = "off" and click_count >= 1 then visit_id end)/count(case when desktop_variant = "off" then impression_count end))-1 as click_rate_pct_change,
	(sum(case when desktop_variant = "on" then total_spend end)/count(case when desktop_variant = "on" then visit_id end))/(sum(case when desktop_variant = "off" then total_spend end)/count(case when desktop_variant = "off" then visit_id end))-1 as spend_pct_change,
	(count(case when desktop_variant = "on" and converted = 1 then visit_id end)/count(case when desktop_variant = "on" then visit_id end))/(count(case when desktop_variant = "off" and converted = 1 then visit_id end)/count(case when desktop_variant = "off" then visit_id end))-1 as visit_cr_change,
	(sum(case when desktop_variant = "on" then total_spend+(total_gms*0.14) end)/count(case when desktop_variant = "on" then visit_id end))/(sum(case when desktop_variant = "off" then total_spend+(total_gms*0.14) end)/count(case when desktop_variant = "off" then visit_id end))-1 as total_revenue_chg
from
	base
group by 1
order by 1
;
