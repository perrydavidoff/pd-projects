with distinct_shops as (
select
	distinct
	a.shop_id,
	b.user_id,
	count(distinct date) as distinct_days,
	sum(revenue) as ea_revenue
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.shop_id = b.shop_id
where
	date between current_date - 60 and current_date - 30 and spend > 0
group by 1,2
),base2 as (
select
	a.shop_id,
	a.ea_revenue,
	distinct_days,
	-- ea_revenue,
	sum(gms_net) as gms_net
from
	distinct_shops a join
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on a.user_id = b.seller_user_id and extract(date from creation_tsz) between current_date - 60 and current_date - 30
group by 1,2,3
)
select
	count(distinct shop_id) as shop_count,
	sum(ea_revenue) as total_ea_revenue,
	sum(gms_net) as total_gms,
	sum(ea_revenue)/sum(gms_net) as gms_share
from
	base2
;

select
	seller_tier_new,
	count(distinct shop_id) as shop_count
from
	`etsy-data-warehouse-prod.rollups.seller_basics`
where
	active_seller_status = 1
group by 1
order by 1
;