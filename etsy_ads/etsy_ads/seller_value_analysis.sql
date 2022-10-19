with date_base as (
select
	distinct
	date(date) as date,
	date_sub(date(date),interval 1 MONTH) as date_1m,
	date_sub(date(date),interval 2 MONTH) as date_2m,
	date_sub(date(date),interval 3 MONTH) as date_3m,
	date_sub(date(date),interval 6 MONTH) as date_6m
from
	`etsy-data-warehouse-prod.public.calendar_dates`
where
	date >= "2022-01-01"
),gms as (
select
	date(creation_tsz) as date,
	seller_user_id,
	-- shop_id,
	sum(gms_net) as gms_2022,
	count(distinct receipt_id) as orders_2022	
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms`
where
	creation_tsz >= "2022-01-01"
group by 1,2
-- ),listing_views as (
-- select
-- 	seller_user_id,
-- 	_date,
-- 	count(case when url like "%plkey%" then listing_id end) as prolist_views,
-- 	count(listing_id) as overall_views
-- from
-- 	`etsy-data-warehouse-prod.analytics.listing_views`
-- group by 1
),imps_visits as (
select
	_date as date,
	user_id,
	search_impressions,
	search_ad_impressions,
	listing_views
from
	`etsy-data-warehouse-prod.analytics.shop_metrics`
where
	_date >= "2022-01-01"
),join_metrics as (
select
	a.shop_id,
	a.date,
	revenue as ea_gms,
	spend,
	budget,
	impression_count,
	click_count,
	converting_clicks,
	orders as ea_orders,
	gms_2022 as total_gms,
	orders_2022 as total_orders,
	search_impressions,
	search_ad_impressions,
	listing_views
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a left join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.shop_id = b.shop_id left join 
	gms c on b.user_id = c.seller_user_id and a.date = c.date left join
	imps_visits d on b.user_id = d.user_id and a.date = d.date
where
	a.date >= "2022-01-01"  
)
select
	shop_id,
	case when sum(case when a.date <= date_3m then spend end)/sum(case when a.date <= date_3m then budget end) >= 0.9 then 1 else 0 end as bc_status_3m,
	-- 3 month ea stats
	sum(case when a.date <= date_3m then budget end) as total_budget_3m,
	sum(case when a.date <= date_3m then spend end) as total_spend_3m,
	safe_divide(sum(case when a.date <= date_3m then spend end),sum(case when a.date <= date_3m then budget end)) as total_budget_util_3m,
	sum(case when a.date <= date_3m then impression_count end) as ea_imps_3m,
	sum(case when a.date <= date_3m then click_count end) as ea_clicks_3m,
	safe_divide(sum(case when a.date <= date_3m then click_count end),sum(case when a.date <= date_3m then impression_count end)) as ea_ctr_3m,
	sum(case when a.date <= date_3m then converting_clicks end) as ea_conv_clicks_3m,
	safe_divide(sum(case when a.date <= date_3m then converting_clicks end),sum(case when a.date <= date_3m then click_count end)) as ea_pccr_3m,
	sum(case when a.date <= date_3m then ea_gms end) as ea_gms_3m,
	safe_divide(sum(case when a.date <= date_3m then ea_gms end),sum(case when a.date <= date_3m then converting_clicks end)) as gms_per_conv_click_3m,
	safe_divide(sum(case when a.date <= date_3m then ea_gms end),sum(case when a.date <= date_3m then spend end)) as roas_3m,
	-- 6 month ea stats
	sum(case when a.date between date_3m and date_6m then budget end) as total_budget_6m,
	sum(case when a.date between date_3m and date_6m then spend end) as total_spend_6m,
	safe_divide(sum(case when a.date between date_3m and date_6m then spend end),sum(case when a.date between date_3m and date_6m then budget end)) as total_budget_util_6m,
	sum(case when a.date between date_3m and date_6m then impression_count end) as ea_imps_6m,
	sum(case when a.date between date_3m and date_6m then click_count end) as ea_clicks_6m,
	safe_divide(sum(case when a.date between date_3m and date_6m then click_count end),sum(case when a.date between date_3m and date_6m then impression_count end)) as ea_ctr_6m,
	sum(case when a.date between date_3m and date_6m then converting_clicks end) as ea_conv_clicks_6m,
	safe_divide(sum(case when a.date between date_3m and date_6m then converting_clicks end),sum(case when a.date between date_3m and date_6m then click_count end)) as ea_pccr_6m,
	sum(case when a.date between date_3m and date_6m then ea_gms end) as ea_gms_6m,
	safe_divide(sum(case when a.date between date_3m and date_6m then ea_gms end),sum(case when a.date between date_3m and date_6m then converting_clicks end)) as gms_per_conv_click_6m,
	safe_divide(sum(case when a.date between date_3m and date_6m then ea_gms end),sum(case when a.date between date_3m and date_6m then spend end)) as roas_6m,
	-- total metrics
	sum(case when a.date <= date_3m then total_gms end) as total_gms_3m,
	sum(case when a.date <= date_3m then total_orders end) as total_orders_3m,
	sum(case when a.date <= date_3m then search_impressions end) as search_imps_3m,
	sum(case when a.date <= date_3m then search_ad_impressions end) as search_ad_imps_3m,
	sum(case when a.date <= date_3m then listing_views end) as listing_views_3m,
	-- total vs. ea comparison
	safe_divide(sum(case when a.date <= date_3m then ea_gms end),sum(case when a.date <= date_3m then total_gms end)) as ea_gms_share_3m,
	safe_divide(sum(case when a.date <= date_3m then ea_orders end),sum(case when a.date <= date_3m then total_orders end)) as ea_orders_share_3m
from
	join_metrics a left join 
	date_base b on a.date = b.date
group by 1
;



