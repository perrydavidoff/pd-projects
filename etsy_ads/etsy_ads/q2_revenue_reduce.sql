with base as (
	select
		date,
		budget_constrained_shop,
		-- shop_id,
		sum(spend) as total_spend,
		count(distinct shop_id) as shop_count,
		sum(budget) as total_budget,
		sum(revenue) as total_revenue
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
where
	date >= current_date - 14
group by 1,2
)
select
	budget_constrained_shop,
	avg(shop_count) as shop_count,
	avg(total_budget) as avg_budget,
	avg(total_spend) as avg_spend,
	avg(total_revenue) as avg_revenue,
from
	base
group by 1
order by 1
;
