-- ea revenue as a share of GMS
with ea_base as (
select
	date_trunc(date,month) as month,
	sum(spend) as total_ea_spend,
	sum(impressions) as total_ea_imps,
	sum(revenue) as total_ea_gms,
	sum(orders) as total_ea_orders,
	sum(clicks) as total_ea_clicks
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary`
where
	date >= "2020-01-01"
group by 1
),overall_etsy_nums as (
select
	date_trunc(date(creation_tsz),month) as month,
	sum(gms_net) as total_etsy_gms,
	count(distinct receipt_id) as total_etsy_orders
from 
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms`
where
	creation_tsz >= "2020-01-01"
group by 1
),listing_views as (
select
	date_trunc(_date,month) as month,
	count(*) as overall_listing_views
from
	`etsy-data-warehouse-prod.analytics.listing_views`
where
	_date >= "2020-01-01"
group by 1
)
select
	a.*,
	b.total_etsy_gms,
	b.total_etsy_orders,
	c.overall_listing_views
from 
	ea_base a join
	overall_etsy_nums b on a.month = b.month join
	listing_views c on a.month = c.month
order by 1
;

-- ea order and gms share just for advertising sellers
with shop_base as (
select
	shop_id,
	date_trunc(date,month) as month,
	sum(spend) as total_ea_spend,
	sum(revenue) as total_ea_gms,
	sum(orders) as total_orders
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
group by 1,2
),base2 as (
select
	shop_id,
	date_trunc(date(creation_tsz),month) as month,
	sum(gms_net) as total_gms,
	count(distinct receipt_id) as total_orders	
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.seller_user_id = b.user_id
group by 1,2
)
select
	a.month,
	count(distinct a.shop_id) as advertising_shop_gms_count,
	sum(b.total_gms) as total_advertsiser_gms,
	sum(b.total_orders) as total_advertiser_orders,
	sum(a.total_ea_gms) as ea_advertiser_gms,
	sum(a.total_orders) as ea_advertiser_orders	
from
	shop_base a join
	base2 b on a.shop_id = b.shop_id and a.month = b.month
group by 1
order by 1
;

-- take rate by seller budget level
with daily_base as (
select
	shop_id,
	date,
    CASE
      WHEN budget <= 5 THEN 'a) $0-$5'
      WHEN budget <= 10 THEN 'b) $5-$10'
      WHEN budget <= 50 THEN 'c) $10-$50'
      WHEN budget <= 100 THEN 'd) $50-$100'
      WHEN budget > 100 THEN 'e) $100+'
    END AS budget_tier,
    spend,
    revenue,
    budget,
    sum(gms_net) as seller_gms
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.shop_id = b.shop_id
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` c on b.user_id = c.seller_user_id and a.date = date(c.creation_tsz)
where
	a.date >= "2020-01-01"
)
select
	date_trunc(month,date) as month,
	budget_tier,
	sum(budget) as total_budget,
	count(distinct shop_id) as shop_count,
	sum(spend) as total_spend,
	sum(revenue) as ea_gms,
	sum(seller_gms) as total_gms,
	sum(revenue)/sum(seller_gms) as ea_gms_share,
	sum(spend)/sum(seller_gms) as ea_take_rate,
	sum(spend)/count(distinct shop_id) as spend_per_shop,
	sum(budget)/count(distinct shop_id) as budget_per_shop
from 
	daily_base
group by 1,2
order by 1,2
;


-- imps by page
select
	date_trunc(_PARTITIONDATE,month) as month,
	page_type,
	count(*) as imp_count,
	count(distinct visit_id) as visits_with_imp
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	_PARTITIONDATE >= "2022-01-01"
group by 1,2
;

-- what is the share of GMS that has come from ads?
-- visit sample
with visit_sample as (
select
	
prolist_platform)
with base as (
select
	visit_id,
	timestamp as imp_time,
	page_type,
	logging_key,
	shop_id,
	listing_id,
	cost,
	predCtr,
	prolist_page,
	prolist_platform,
	predCvr	
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	_PARTITIONDATE >= "2022-01-01"
),imp_counts as (
select
	visit_id,
	prolist_platform,
	count(*) as impression_count,
	count(case when page_type = "0" then visit_id end) as search_imps,
	count(case when page_type = "1" then visit_id end) as market_imps,
	count(case when page_type = "8" then visit_id end) as listing_imps,
	count(case when page_type = "7" then visit_id end) as similar_pla_sash_imps,
	count(case when page_type = "2" then visit_id end) as category_imps,
	count(case when page_type = "10" then visit_id end) as search_strv_imps,
	count(case when page_type = "12" then visit_id end) as web_home_strv_imps,
	count(case when page_type = "9" then visit_id end) as nla_listing_imps,
	count(case when page_type = "4" then visit_id end) as similar_listing_imps,
	count(case when page_type = "13" then visit_id end) as boe_visually_similar_imps,
	count(case when page_type = "11" then visit_id end) as boe_home_strv_imps


from
	)
),visit_base as (
select
	)