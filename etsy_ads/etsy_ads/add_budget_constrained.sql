create or replace table
  `etsy-data-warehouse-dev.pdavidoff.budget_30d`
  as (
select
	*,
	count(date) over(partition by shop_id order by unix_date(date) RANGE BETWEEN 29 PRECEDING and current row) as active_budget_days_30d,
	coalesce(sum(spend) over(partition by shop_id order by unix_date(date) RANGE BETWEEN 29 PRECEDING AND current row),0) as spend_30d,
	coalesce(sum(budget) over(partition by shop_id order by unix_date(date) RANGE BETWEEN 29 PRECEDING AND current row),0) as budget_30d,
	safe_divide(coalesce(sum(spend) over(partition by shop_id order by unix_date(date) RANGE BETWEEN 29 PRECEDING AND current row),0), 
		coalesce(sum(budget) over(partition by shop_id order by unix_date(date) RANGE BETWEEN 29 PRECEDING AND current row),0)) as budget_util_30d,
	case 
		when safe_divide(coalesce(sum(spend) over(partition by shop_id order by unix_date(date) RANGE BETWEEN 29 PRECEDING AND current row),0), 
		coalesce(sum(budget) over(partition by shop_id order by unix_date(date) RANGE BETWEEN 29 PRECEDING AND current row),0)) > 0.9 
		then 1 else 0 
	end as budget_constrained_shop
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
)
;

select
	date,
	shop_id,
	spend,
	budget,
	spend_30d,
	budget_30d,
	budget_constrained_shop,
	active_budget_days_30d
from
	`etsy-data-warehouse-dev.pdavidoff.budget_30d`
where
	date >= "2022-01-01"
order by shop_id,date
limit 50;

select
	date,
	spend,
	shop_id,
	spend_last_4w
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
limit 50;

