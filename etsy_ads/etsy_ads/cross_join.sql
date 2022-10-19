
create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.ea_constrained_seller_imps`
	as (
-- get first date for each seller
with first_prolist_date as (
select
	shop_id,
	min(date) as first_prolist_date
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
group by 1
-- join first date to their daily budget
),daily_data_join as (
select
	a.date,
	a.shop_id,
	a.budget,
	b.first_prolist_date	
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a left join
	first_prolist_date b on a.shop_id = b.shop_id
),cross_join as (
-- create a row for every seller and day after their first prolist date, even if their budget is 0
select
	distinct
	a.shop_id,
	b.date,
	case when a.budget is not null then a.budget else 0 end as budget
from
	daily_data_join a cross join
	`etsy-data-warehouse-prod.public.calendar_dates` b
where
	date(b.date) >= date(a.first_prolist_date)
)
-- sum their budget over prior 30 days
select
	a.date,
	shop_id,
	budget,
	sum(a.budget) OVER (PARTITION BY a.shop_id ORDER BY unix_seconds(b.date) range between 2592000 preceding and 86400 preceding) AS sum_of_budget_30_days,
from
	cross_join a left join
	`etsy-data-warehouse-prod.public.calendar_dates` as b on TIMESTAMP(a.date) = b.date
order by 1 desc
)
;


