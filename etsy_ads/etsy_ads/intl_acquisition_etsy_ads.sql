-- int'l seller metrics
select
	c.iso_country_code as country_code,
	count(distinct a.shop_id) as shop_count,
	count(distinct b.shop_id) as etsy_ads_shop_count,
	count(distinct case when b.budget_constrained_shop = 1 then b.shop_id end) as ea_bc_status,
	avg(b.budget) as avg_budget,
	avg(b.spend) as avg_spend
from
	`etsy-data-warehouse-prod.rollups.seller_basics` a left join
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` b on a.shop_id = b.shop_id and b.date = current_date - 2 join
	`etsy-data-warehouse-prod.etsy_v2.countries` c on a.country_id = c.country_id and c.iso_country_code in ("US","GB","DE","FR","CA","IN")
where
	sws_status = 1
group by 1
order by 2 desc
;

select
	count(distinct shop_id)
from
	`etsy-data-warehouse-prod.rollups.seller_basics`
where
	active_seller_status = 1
;