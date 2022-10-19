select
	b.top_category_new,
	count(distinct a.shop_id) as all_shops,
	sum(budget) as all_budget,
	sum(spend) as all_spend,
	sum(impression_count) as all_imps,		
	count(distinct case when budget_constrained_shop = 1 then a.shop_id end) as bc_shops,
	sum(case when budget_constrained_shop = 1 then budget end) as bc_budget,
	sum(case when budget_constrained_shop = 1 then spend end) as bc_spend,
	sum(case when budget_constrained_shop = 1 then impression_count end) as bc_impressions,	
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.shop_id = b.shop_id
where
	a.date >= current_date - 30
group by 1
order by 2 desc
;
