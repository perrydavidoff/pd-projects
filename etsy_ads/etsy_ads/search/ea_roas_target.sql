-- goal: analyze how different ROAS levels impact
-- 	- budget levels in ads
-- 	- churn
-- 	- seller satisfaction


-- what is the distribution of ROAS levels today?
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
  as (
  	with month_seller_tier as (
  		select
  			shop_id,
  			seller_tier_new,
  			date_trunc(date,month) as month,
  			rank() over(partition by shop_id,date_trunc(date,month) order by date) as rank
  		from
  			`etsy-data-warehouse-prod.rollups.seller_tier_new_daily_historical`
  		where
  			date >= "2017-01-01"
  		qualify rank = 1
  	),monthly_roas_group as (
  	select
  		date_trunc(date,month) as month,
  		shop_id,
  		safe_divide(sum(revenue),sum(spend)) as roas,
  		sum(budget) as total_budget,
  		sum(revenue) as total_ea_revenue,
  		sum(spend) as total_spend,
  		count(distinct date) as active_budget_days,
  		safe_divide(sum(spend),sum(budget)) as budget_util,
  		case when safe_divide(sum(spend),sum(budget)) >= 0.9 then 1 else 0 end as budget_constrained_shop
  	from
  		`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
  	where
  		date >= "2017-01-01"
  	group by 1,2
  	),base2 as (
  	select
  		a.*,
  		c.user_id as seller_user_id,
  		b.seller_tier_new,
  		case
			when roas < 1 then "1. ROAS 0-1"
			when roas between 1 and 2 then "2. ROAS 1-2"
			when roas between 2 and 3 then "3. ROAS 2-3"
			when roas between 3 and 4 then "4. ROAS 3-4"
			when roas between 4 and 5 then "5. ROAS 4-5"
			when roas between 5 and 6 then "6. ROAS 5-6"
			when roas between 6 and 7 then "7. ROAS 6-7"
			when roas >7 then "8. ROAS 7+"
		end as roas_group
	from
		monthly_roas_group a left join
		month_seller_tier b on a.shop_id = b.shop_id and a.month = b.month left join
		`etsy-data-warehouse-prod.rollups.seller_basics` c on a.shop_id = c.shop_id
	),base3 as (
	select
		a.*,
		sum(gms_gross) as total_gms
	from
		base2 a left join
		`etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on a.seller_user_id = b.seller_user_id and a.month = date_trunc(date(creation_tsz),month)
	group by 1,2,3,4,5,6,7,8,9,10,11,12
	)
	select
		*,
		safe_divide(total_ea_revenue,total_gms) as share_gms_to_ea,
		case 
			when safe_divide(total_ea_revenue,total_gms) = 0 then "1. 0% GMS Share"
			when safe_divide(total_ea_revenue,total_gms) between 0 and 0.20 then "2. 0%-20% GMS Share"
			when safe_divide(total_ea_revenue,total_gms) between 0.20 and 0.40 then "3. 20%-40% GMS Share"
			when safe_divide(total_ea_revenue,total_gms) between 0.40 and 0.60 then "4. 40%-60% GMS Share"
			when safe_divide(total_ea_revenue,total_gms) between 0.60 and 0.80 then "5. 60%-80% GMS Share"
			when safe_divide(total_ea_revenue,total_gms) between 0.80 and 0.99 then "6. 80%-100% GMS Share"
			when safe_divide(total_ea_revenue,total_gms) > 0.99 then "7. 100% GMS Share"
		end as gms_share_group
	from
		base3
)
  ;

-- breakout of shops by budget constrained status and ROAS
select
	budget_constrained_shop,
	roas_group,
	gms_share_group,
	count(distinct shop_id) as shop_count,
	sum(total_budget) as total_budget,
	sum(total_spend) as total_spend
from 
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month = "2022-04-01"
group by 1,2,3
order by 1,2,3
;
-- distribution of sellers/spend by roas group
select
	month,
	roas_group,
	count(distinct shop_id) as shop_count,
	sum(total_spend) as total_spend,
	sum(total_budget) as total_budget,
	count(distinct case when budget_constrained_shop = 1 then shop_id end) as bc_shops,
	sum(total_ea_revenue) as total_ea_revenue,
	sum(total_gms) as total_gms,
	sum(active_budget_days) as budget_days_per_seller,
	count(distinct case when seller_tier_new in ("Top Shop","Power Shop") then shop_id end) as top_power_shop_share
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
group by 1,2
order by 1,2
;

-- what percent of budget comes from top/power sellers
select
	sum(case when seller_tier_new in ("Top Shop","Power Shop") then total_budget end)/sum(total_budget) as top_power_budget_share
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month = "2021-04-01"
;


-- show progression of budget for sellers at different roas groups a year ago
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
)
select
	roas_group,
	count(distinct shop_id) as shop_count,
	sum(total_budget) as total_budget,
	avg(total_budget) as month_0,
	avg(budget_month_1) as month_1,
	avg(budget_month_2) as month_2,
	avg(budget_month_3) as month_3,
	avg(budget_month_4) as month_4,
	avg(budget_month_5) as month_5,
	avg(budget_month_6) as month_6,
	avg(budget_month_7) as month_7,
	avg(budget_month_8) as month_8,
	avg(budget_month_9) as month_9,
	avg(budget_month_10) as month_10,
	avg(budget_month_11) as month_11
from
	base
where
	month = "2021-04-01"
group by 1
order by 1
;


-- look just at top and power sellers
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
)
select
	roas_group,
	count(distinct shop_id) as shop_count,
	sum(total_budget) as total_budget,
	avg(total_budget) as month_0,
	avg(budget_month_1) as month_1,
	avg(budget_month_2) as month_2,
	avg(budget_month_3) as month_3,
	avg(budget_month_4) as month_4,
	avg(budget_month_5) as month_5,
	avg(budget_month_6) as month_6,
	avg(budget_month_7) as month_7,
	avg(budget_month_8) as month_8,
	avg(budget_month_9) as month_9,
	avg(budget_month_10) as month_10,
	avg(budget_month_11) as month_11
from
	base
where
	month = "2021-04-01" and seller_tier_new in ("Top Shop","Power Shop")
group by 1
order by 1
;

-- gms share group
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
)
select
	gms_share_group,
	count(distinct shop_id) as shop_count,
	sum(total_budget) as total_budget,
	avg(total_budget) as month_0,
	avg(budget_month_1) as month_1,
	avg(budget_month_2) as month_2,
	avg(budget_month_3) as month_3,
	avg(budget_month_4) as month_4,
	avg(budget_month_5) as month_5,
	avg(budget_month_6) as month_6,
	avg(budget_month_7) as month_7,
	avg(budget_month_8) as month_8,
	avg(budget_month_9) as month_9,
	avg(budget_month_10) as month_10,
	avg(budget_month_11) as month_11
from
	base
where
	month = "2021-04-01" and seller_tier_new in ("Top Shop","Power Shop")
group by 1
order by 1
;


-- seller retention
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
)
select
	roas_group,
	sum(total_budget) as total_budget,
	count(distinct shop_id) as shop_count,
	count(distinct case when budget_month_1 > 0 then shop_id end) as month_1,
	count(distinct case when budget_month_2 > 0 then shop_id end) as month_2,
	count(distinct case when budget_month_3 > 0 then shop_id end) as month_3,
	count(distinct case when budget_month_4 > 0 then shop_id end) as month_4,
	count(distinct case when budget_month_5 > 0 then shop_id end) as month_5,
	count(distinct case when budget_month_6 > 0 then shop_id end) as month_6,
	count(distinct case when budget_month_7 > 0 then shop_id end) as month_7,
	count(distinct case when budget_month_8 > 0 then shop_id end) as month_8,
	count(distinct case when budget_month_9 > 0 then shop_id end) as month_9,
	count(distinct case when budget_month_10 > 0 then shop_id end) as month_10,
	count(distinct case when budget_month_11 > 0 then shop_id end) as month_11
from 
	base
where
	month = "2021-04-01"
group by 1
order by 1
;

-- how much seller change is there?
-- sankey chart
with base as (
select
	month,
	roas_group as roas_group_source,
	case when lead(roas_group,1) over(partition by shop_id order by month) is not null then lead(roas_group,1) over(partition by shop_id order by month) else "Churned" end as next_roas_group,
	shop_id,
	total_budget
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month = "2021-04-01" and roas_group is not null
limit 50;
)
select
	roas_group_source,
	next_roas_group,
	count(distinct shop_id) as shop_count,
	sum(total_budget) as total_budget
from
	base 
group by 1,2
order by 1,2
;

-- what share of budget in each ROAS group is from new sellers, existing sellers with higher roas, lower roas or same roas
with base as (
select
	shop_id,
	min(month) as first_month
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
group by 1
),base2 as (
select
	a.*,
	b.first_month,
	total_ea_revenue,
	coalesce(lag(total_budget,1) over(partition by a.shop_id order by month),0) as budget_last_month,
	coalesce(lag(roas,1) over(partition by a.shop_id order by month),0) as roas_last_month
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis` a join
	base b on a.shop_id = b.shop_id
),classify as (
select
	*,
	case 
		when first_month = month then "New"
		when coalesce(round(roas,0),0) = coalesce(round(roas_last_month,0),0) then "Same ROAS"
		when coalesce(round(roas,0),0) > coalesce(round(roas_last_month,0),0) then "Higher ROAS"
		when coalesce(round(roas,0),0) < coalesce(round(roas_last_month,0),0) then "Lower ROAS"
		-- when total_spend = 0 then "No Spend"
	end as seller_type
from 
	base2
)
select
	roas_group,
	sum(case when seller_type = "New" then total_budget end) as new_budget,
	sum(case when seller_type = "Same ROAS" then total_budget end) as same_roas_budget,
	sum(case when seller_type = "Higher ROAS" then total_budget end) as higher_roas_budget,
	sum(case when seller_type = "Lower ROAS" then total_budget end) as lower_roas_budget
	-- sum(case when seller_type = "No Spend" then total_budget end) as no_spend_budget
from 
	classify
where
	month >= "2022-04-01"
group by 1
order by 1
;


-- create a matrix of 6 months
with join_date as (
select
	shop_id,
	min(date_trunc(date,month)) as join_month
from 
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
group by 1
),base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	-- coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	-- coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	-- coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	-- coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	-- coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11,
	lead(roas_group,1) over(partition by shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by shop_id order by month) as roas_group_month_5
	-- lead(roas_group,6) over(partition by shop_id order by month) as roas_group_month_6,
	-- lead(roas_group,7) over(partition by shop_id order by month) as roas_group_month_7,
	-- lead(roas_group,8) over(partition by shop_id order by month) as roas_group_month_8,
	-- lead(roas_group,9) over(partition by shop_id order by month) as roas_group_month_9,
	-- lead(roas_group,10) over(partition by shop_id order by month) as roas_group_month_10,
	-- lead(roas_group,11) over(partition by shop_id order by month) as roas_group_month_11
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis` a join 
	join_date b on a.shop_id = b.shop_id
where
	month >= "2021-04-01"
)
select
	roas_group,
	case when roas_group_month_1 is null then "Churned" else roas_group_month_1 end as roas_group_month_1,
	case when roas_group_month_2 is null then "Churned" else roas_group_month_2 end as roas_group_month_2,
	case when roas_group_month_3 is null then "Churned" else roas_group_month_3 end as roas_group_month_3,
	case when roas_group_month_4 is null then "Churned" else roas_group_month_4 end as roas_group_month_4,
	case when roas_group_month_5 is null then "Churned" else roas_group_month_5 end as roas_group_month_5,
	-- roas_group_month_6,
	-- roas_group_month_7,
	-- roas_group_month_8,
	-- roas_group_month_9,
	-- roas_group_month_10,
	-- roas_group_month_11,
	sum(total_budget) as total_budget,
	sum(budget_month_1) as total_budget_month_1,
	sum(budget_month_2) as total_budget_month_2,
	sum(budget_month_3) as total_budget_month_3,
	sum(budget_month_4) as total_budget_month_4,
	sum(budget_month_5) as total_budget_month_5,
	-- sum(budget_month_6) as total_budget_month_6,
	-- sum(budget_month_7) as total_budget_month_7,
	-- sum(budget_month_8) as total_budget_month_8,
	-- sum(budget_month_9) as total_budget_month_9,
	-- sum(budget_month_10) as total_budget_month_10,
	-- sum(budget_month_11) as total_budget_month_11,
	count(distinct shop_id) as total_shops,
	count(distinct case when budget_month_1 > 0 then shop_id end) as retained_shops_month_1,
	count(distinct case when budget_month_2 > 0 then shop_id end) as retained_shops_month_2,
	count(distinct case when budget_month_3 > 0 then shop_id end) as retained_shops_month_3,
	count(distinct case when budget_month_4 > 0 then shop_id end) as retained_shops_month_4,
	count(distinct case when budget_month_5 > 0 then shop_id end) as retained_shops_month_5
	-- count(distinct case when budget_month_6 > 0 then shop_id end) as retained_shops_month_6,
	-- count(distinct case when budget_month_7 > 0 then shop_id end) as retained_shops_month_7,
	-- count(distinct case when budget_month_8 > 0 then shop_id end) as retained_shops_month_8,
	-- count(distinct case when budget_month_9 > 0 then shop_id end) as retained_shops_month_9,
	-- count(distinct case when budget_month_10 > 0 then shop_id end) as retained_shops_month_10,
	-- count(distinct case when budget_month_11 > 0 then shop_id end) as retained_shops_month_11
from
	base 
where
	month = "2021-04-01"
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
;


-- do a similar matrix, but do the future 3 months
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	-- coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	-- coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	-- coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	-- coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11,
	lead(roas_group,1) over(partition by shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by shop_id order by month) as roas_group_month_5,
	-- lead(roas_group,6) over(partition by shop_id order by month) as roas_group_month_6,
	-- lead(roas_group,7) over(partition by shop_id order by month) as roas_group_month_7,
	-- lead(roas_group,8) over(partition by shop_id order by month) as roas_group_month_8,
	-- lead(roas_group,9) over(partition by shop_id order by month) as roas_group_month_9,
	-- lead(roas_group,10) over(partition by shop_id order by month) as roas_group_month_10,
	-- lead(roas_group,11) over(partition by shop_id order by month) as roas_group_month_11
	coalesce(lead(total_ea_revenue,1) over(partition by shop_id order by month),0) as ea_revenue_month_1,
	coalesce(lead(total_ea_revenue,2) over(partition by shop_id order by month),0) as ea_revenue_month_2,
	coalesce(lead(total_ea_revenue,3) over(partition by shop_id order by month),0) as ea_revenue_month_3,
	coalesce(lead(total_ea_revenue,4) over(partition by shop_id order by month),0) as ea_revenue_month_4,
	coalesce(lead(total_ea_revenue,5) over(partition by shop_id order by month),0) as ea_revenue_month_5,
	coalesce(lead(total_spend,1) over(partition by shop_id order by month),0) as total_spend_month_1,
	coalesce(lead(total_spend,2) over(partition by shop_id order by month),0) as total_spend_month_2,
	coalesce(lead(total_spend,3) over(partition by shop_id order by month),0) as total_spend_month_3,
	coalesce(lead(total_spend,4) over(partition by shop_id order by month),0) as total_spend_month_4,
	coalesce(lead(total_spend,5) over(partition by shop_id order by month),0) as total_spend_month_5
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
),shop_level_roas as (
select
	*,
	case 
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 0 and 1 then "1. ROAS 0-1"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 1 and 2 then "2. ROAS 1-2"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 2 and 3 then "3. ROAS 2-3"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 3 and 4 then "4. ROAS 3-4"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 4 and 5 then "5. ROAS 4-5"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 5 and 6 then "6. ROAS 5-6"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 6 and 7 then "7. ROAS 6-7"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) >= 7 then "8. ROAS 7+"
	end as roas_group_3mo,
	case 
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 0 and 1 then "1. ROAS 0-1"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 1 and 2 then "2. ROAS 1-2"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 2 and 3 then "3. ROAS 2-3"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 3 and 4 then "4. ROAS 3-4"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 4 and 5 then "5. ROAS 4-5"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 5 and 6 then "6. ROAS 5-6"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 6 and 7 then "7. ROAS 6-7"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) >= 7 then "8. ROAS 7+"
	end as roas_group_6mo
from
	base 
)
select
	roas_group,
	roas_group_3mo,
	roas_group_6mo,
	-- case when budget_month_3 = 0 then "Churned" else roas_group_3mo end as roas_group_3mo,
	-- case when budget_month_6 = 0 then "Churned" else roas_group_6mo end as roas_group_6mo,
	sum(total_budget) as total_budget,
	sum(budget_month_1) as total_budget_month_1,
	sum(budget_month_3) as total_budget_month_3,
	sum(budget_month_6) as total_budget_month_6,
	-- sum(budget_month_2) as budget_3mo,
	count(distinct shop_id) as shop_count
from
	shop_level_roas
where
	month = "2021-04-01" and roas_group is not null
group by 1,2,3
order by 1,2,3
;

with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	-- coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	-- coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	-- coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	-- coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11,
	lead(roas_group,1) over(partition by shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by shop_id order by month) as roas_group_month_5,
	-- lead(roas_group,6) over(partition by shop_id order by month) as roas_group_month_6,
	-- lead(roas_group,7) over(partition by shop_id order by month) as roas_group_month_7,
	-- lead(roas_group,8) over(partition by shop_id order by month) as roas_group_month_8,
	-- lead(roas_group,9) over(partition by shop_id order by month) as roas_group_month_9,
	-- lead(roas_group,10) over(partition by shop_id order by month) as roas_group_month_10,
	-- lead(roas_group,11) over(partition by shop_id order by month) as roas_group_month_11
	coalesce(lead(total_ea_revenue,1) over(partition by shop_id order by month),0) as ea_revenue_month_1,
	coalesce(lead(total_ea_revenue,2) over(partition by shop_id order by month),0) as ea_revenue_month_2,
	coalesce(lead(total_ea_revenue,3) over(partition by shop_id order by month),0) as ea_revenue_month_3,
	coalesce(lead(total_ea_revenue,4) over(partition by shop_id order by month),0) as ea_revenue_month_4,
	coalesce(lead(total_ea_revenue,5) over(partition by shop_id order by month),0) as ea_revenue_month_5,
	coalesce(lead(total_spend,1) over(partition by shop_id order by month),0) as total_spend_month_1,
	coalesce(lead(total_spend,2) over(partition by shop_id order by month),0) as total_spend_month_2,
	coalesce(lead(total_spend,3) over(partition by shop_id order by month),0) as total_spend_month_3,
	coalesce(lead(total_spend,4) over(partition by shop_id order by month),0) as total_spend_month_4,
	coalesce(lead(total_spend,5) over(partition by shop_id order by month),0) as total_spend_month_5
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
),shop_level_roas as (
select
	*,
	case 
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 0 and 1 then "1. ROAS 0-1"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 1 and 2 then "2. ROAS 1-2"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 2 and 3 then "3. ROAS 2-3"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 3 and 4 then "4. ROAS 3-4"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 4 and 5 then "5. ROAS 4-5"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 5 and 6 then "6. ROAS 5-6"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 6 and 7 then "7. ROAS 6-7"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) >= 7 then "8. ROAS 7+"
	end as roas_group_3mo,
	case 
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 0 and 1 then "1. ROAS 0-1"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 1 and 2 then "2. ROAS 1-2"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 2 and 3 then "3. ROAS 2-3"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 3 and 4 then "4. ROAS 3-4"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 4 and 5 then "5. ROAS 4-5"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 5 and 6 then "6. ROAS 5-6"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 6 and 7 then "7. ROAS 6-7"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) >= 7 then "8. ROAS 7+"
	end as roas_group_6mo
from
	base 
)
select
	roas_group,
	ras_group_3mo,
	roas_group_6mo,
	sum(budget) as total_budget,
	sum(budget_month_1) as total_budget_month_1,
	sum(budget_month_3) as total_budget_month_3,
	sum(budget_month_6) as total_budget_month_6,
	count(distinct shop_id) as shop_count 
from 
	shop_level_roas
where
	budget_constrained_shop = 1
group by 1,2,3
;
-- matrix for GMS share
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	lead(roas_group,1) over(partition by shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by shop_id order by month) as roas_group_month_5,
	coalesce(lead(total_ea_revenue,1) over(partition by shop_id order by month),0) as ea_revenue_month_1,
	coalesce(lead(total_ea_revenue,2) over(partition by shop_id order by month),0) as ea_revenue_month_2,
	coalesce(lead(total_ea_revenue,3) over(partition by shop_id order by month),0) as ea_revenue_month_3,
	coalesce(lead(total_ea_revenue,4) over(partition by shop_id order by month),0) as ea_revenue_month_4,
	coalesce(lead(total_ea_revenue,5) over(partition by shop_id order by month),0) as ea_revenue_month_5,
	coalesce(lead(total_gms,1) over(partition by shop_id order by month),0) as total_gms_month_1,
	coalesce(lead(total_gms,2) over(partition by shop_id order by month),0) as total_gms_month_2,
	coalesce(lead(total_gms,3) over(partition by shop_id order by month),0) as total_gms_month_3,
	coalesce(lead(total_gms,4) over(partition by shop_id order by month),0) as total_gms_month_4,
	coalesce(lead(total_gms,5) over(partition by shop_id order by month),0) as total_gms_month_5
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
),shop_level_roas as (
select
	*,
	case 
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) = 0 then "1. 0% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_3mo,
	case 
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) = 0 then "1. 0% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_6mo
from
	base 
)
select
	gms_share_group,
	gms_share_group_3mo,
	gms_share_group_6mo,
	-- case when budget_month_3 = 0 then "Churned" else roas_group_3mo end as roas_group_3mo,
	-- case when budget_month_6 = 0 then "Churned" else roas_group_6mo end as roas_group_6mo,
	sum(total_budget) as total_budget,
	sum(budget_month_1) as total_budget_month_1,
	sum(budget_month_3) as total_budget_month_3,
	sum(budget_month_6) as total_budget_month_6,
	-- sum(budget_month_2) as budget_3mo,
	count(distinct shop_id) as shop_count
from
	shop_level_roas
where
	month = "2021-04-01" and gms_share_group is not null
group by 1,2,3
order by 1,2,3
;

-- 6 month matrix for bc sellers
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	lead(roas_group,1) over(partition by shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by shop_id order by month) as roas_group_month_5,
	coalesce(lead(total_ea_revenue,1) over(partition by shop_id order by month),0) as ea_revenue_month_1,
	coalesce(lead(total_ea_revenue,2) over(partition by shop_id order by month),0) as ea_revenue_month_2,
	coalesce(lead(total_ea_revenue,3) over(partition by shop_id order by month),0) as ea_revenue_month_3,
	coalesce(lead(total_ea_revenue,4) over(partition by shop_id order by month),0) as ea_revenue_month_4,
	coalesce(lead(total_ea_revenue,5) over(partition by shop_id order by month),0) as ea_revenue_month_5,
	coalesce(lead(total_gms,1) over(partition by shop_id order by month),0) as total_gms_month_1,
	coalesce(lead(total_gms,2) over(partition by shop_id order by month),0) as total_gms_month_2,
	coalesce(lead(total_gms,3) over(partition by shop_id order by month),0) as total_gms_month_3,
	coalesce(lead(total_gms,4) over(partition by shop_id order by month),0) as total_gms_month_4,
	coalesce(lead(total_gms,5) over(partition by shop_id order by month),0) as total_gms_month_5
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
),shop_level_roas as (
select
	*,
	case 
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) = 0 then "1. 0% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_3mo,
	case 
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) = 0 then "1. 0% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_6mo
from
	base 
)
select
	gms_share_group,
	gms_share_group_3mo,
	gms_share_group_6mo,
	-- case when budget_month_3 = 0 then "Churned" else roas_group_3mo end as roas_group_3mo,
	-- case when budget_month_6 = 0 then "Churned" else roas_group_6mo end as roas_group_6mo,
	sum(total_budget) as total_budget,
	sum(budget_month_1) as total_budget_month_1,
	sum(budget_month_3) as total_budget_month_3,
	sum(budget_month_6) as total_budget_month_6,
	-- sum(budget_month_2) as budget_3mo,
	count(distinct shop_id) as shop_count
from
	shop_level_roas
where
	month = "2021-04-01" and gms_share_group is not null and budget_constrained_shop = 1
group by 1,2,3
order by 1,2,3
;


-- 6 month roas by GMS share group
with join_date as (
select
	shop_id,
	min(date_trunc(date,month)) as join_month
from 
	`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
group by 1
),base as (
select
	a.*,
	date_diff(month,join_month,month) as month_diff,
	coalesce(lead(total_budget,1) over(partition by a.shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by a.shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by a.shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by a.shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by a.shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by a.shop_id order by month),0) as budget_month_6,
	lead(roas_group,1) over(partition by a.shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by a.shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by a.shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by a.shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by a.shop_id order by month) as roas_group_month_5,
	coalesce(lead(total_ea_revenue,1) over(partition by a.shop_id order by month),0) as ea_revenue_month_1,
	coalesce(lead(total_ea_revenue,2) over(partition by a.shop_id order by month),0) as ea_revenue_month_2,
	coalesce(lead(total_ea_revenue,3) over(partition by a.shop_id order by month),0) as ea_revenue_month_3,
	coalesce(lead(total_ea_revenue,4) over(partition by a.shop_id order by month),0) as ea_revenue_month_4,
	coalesce(lead(total_ea_revenue,5) over(partition by a.shop_id order by month),0) as ea_revenue_month_5,
	coalesce(lead(total_gms,1) over(partition by a.shop_id order by month),0) as total_gms_month_1,
	coalesce(lead(total_gms,2) over(partition by a.shop_id order by month),0) as total_gms_month_2,
	coalesce(lead(total_gms,3) over(partition by a.shop_id order by month),0) as total_gms_month_3,
	coalesce(lead(total_gms,4) over(partition by a.shop_id order by month),0) as total_gms_month_4,
	coalesce(lead(total_gms,5) over(partition by a.shop_id order by month),0) as total_gms_month_5,
	coalesce(lead(total_spend,1) over(partition by a.shop_id order by month),0) as total_spend_month_1,
	coalesce(lead(total_spend,2) over(partition by a.shop_id order by month),0) as total_spend_month_2,
	coalesce(lead(total_spend,3) over(partition by a.shop_id order by month),0) as total_spend_month_3,
	coalesce(lead(total_spend,4) over(partition by a.shop_id order by month),0) as total_spend_month_4,
	coalesce(lead(total_spend,5) over(partition by a.shop_id order by month),0) as total_spend_month_5
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis` a join 
	join_date b on a.shop_id = b.shop_id
where
	month >= "2021-04-01"
),shop_level_roas as (
select
	*,
	case 
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) = 0 then "1. 0% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2)) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_3mo,
	case 
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) = 0 then "1. 0% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when 1/(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5)) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_6mo
from
	base 
)
select
	-- gms_share_group,
	-- gms_share_group_3mo,
	gms_share_group_6mo,
	-- case when budget_month_3 = 0 then "Churned" else roas_group_3mo end as roas_group_3mo,
	-- case when budget_month_6 = 0 then "Churned" else roas_group_6mo end as roas_group_6mo,
	sum(total_budget) as total_budget,
	sum(budget_month_1) as total_budget_month_1,
	sum(budget_month_3) as total_budget_month_3,
	sum(budget_month_6) as total_budget_month_6,
	sum(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5) as total_ea_revenue,
	sum(total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) as total_ea_spend,
	avg(month_diff) as avg_months,
	sum(case when seller_tier_new in ("Top Shop","Power Shop") then budget_month_6 end)/sum(budget_month_6) as budget_share_top_power,
	-- sum(budget_month_2) as budget_3mo,
	count(distinct shop_id) as shop_count
from
	shop_level_roas
where
	month = "2021-04-01" and gms_share_group is not null
group by 1
order by 1
;

-- ROAS and GMS group matrix --> 6 months in the future
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	-- coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	-- coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	-- coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	-- coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11,
	lead(roas_group,1) over(partition by shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by shop_id order by month) as roas_group_month_5,
	-- lead(roas_group,6) over(partition by shop_id order by month) as roas_group_month_6,
	-- lead(roas_group,7) over(partition by shop_id order by month) as roas_group_month_7,
	-- lead(roas_group,8) over(partition by shop_id order by month) as roas_group_month_8,
	-- lead(roas_group,9) over(partition by shop_id order by month) as roas_group_month_9,
	-- lead(roas_group,10) over(partition by shop_id order by month) as roas_group_month_10,
	-- lead(roas_group,11) over(partition by shop_id order by month) as roas_group_month_11
	coalesce(lead(total_ea_revenue,1) over(partition by shop_id order by month),0) as ea_revenue_month_1,
	coalesce(lead(total_ea_revenue,2) over(partition by shop_id order by month),0) as ea_revenue_month_2,
	coalesce(lead(total_ea_revenue,3) over(partition by shop_id order by month),0) as ea_revenue_month_3,
	coalesce(lead(total_ea_revenue,4) over(partition by shop_id order by month),0) as ea_revenue_month_4,
	coalesce(lead(total_ea_revenue,5) over(partition by shop_id order by month),0) as ea_revenue_month_5,
	coalesce(lead(total_spend,1) over(partition by shop_id order by month),0) as total_spend_month_1,
	coalesce(lead(total_spend,2) over(partition by shop_id order by month),0) as total_spend_month_2,
	coalesce(lead(total_spend,3) over(partition by shop_id order by month),0) as total_spend_month_3,
	coalesce(lead(total_spend,4) over(partition by shop_id order by month),0) as total_spend_month_4,
	coalesce(lead(total_spend,5) over(partition by shop_id order by month),0) as total_spend_month_5,
	coalesce(lead(total_gms,1) over(partition by shop_id order by month),0) as total_gms_month_1,
	coalesce(lead(total_gms,2) over(partition by shop_id order by month),0) as total_gms_month_2,
	coalesce(lead(total_gms,3) over(partition by shop_id order by month),0) as total_gms_month_3,
	coalesce(lead(total_gms,4) over(partition by shop_id order by month),0) as total_gms_month_4,
	coalesce(lead(total_gms,5) over(partition by shop_id order by month),0) as total_gms_month_5
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
),shop_level_roas as (
select
	*,
	-- case 
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 0 and 1 then "1. ROAS 0-1"
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 1 and 2 then "2. ROAS 1-2"
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 2 and 3 then "3. ROAS 2-3"
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 3 and 4 then "4. ROAS 3-4"
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 4 and 5 then "5. ROAS 4-5"
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 5 and 6 then "6. ROAS 5-6"
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) between 6 and 7 then "7. ROAS 6-7"
	-- 	when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2,total_spend+total_spend_month_1+total_spend_month_2) >= 7 then "8. ROAS 7+"
	-- end as roas_group_3mo,
	case 
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 0 and 1 then "1. ROAS 0-1"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 1 and 2 then "2. ROAS 1-2"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 2 and 3 then "3. ROAS 2-3"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 3 and 4 then "4. ROAS 3-4"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 4 and 5 then "5. ROAS 4-5"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 5 and 6 then "6. ROAS 5-6"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 6 and 7 then "7. ROAS 6-7"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) >= 7 then "8. ROAS 7+"
	end as roas_group_6mo,
	case 
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) = 0 then "1. 0% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_6mo
from
	base
)
select
	case when roas_group_6mo is null or gms_share_group_6mo is null then "1. No Sales" else roas_group_6mo end as roas_group_6mo,
	case when gms_share_group_6mo is null then "1. 0% GMS Share" else gms_share_group_6mo end as gms_share_group_6mo,
	-- case when seller_tier_new in ("Top Shop","Power Shop") then 1 else 0 end as top_seller_status,
	-- case when budget_month_3 = 0 then "Churned" else roas_group_3mo end as roas_group_3mo,
	-- case when budget_month_6 = 0 then "Churned" else roas_group_6mo end as roas_group_6mo,
	sum(total_budget) as total_budget,
	sum(budget_month_1) as total_budget_month_1,
	sum(budget_month_3) as total_budget_month_3,
	sum(budget_month_6) as total_budget_month_6,	
	-- sum(budget_month_2) as budget_3mo,
	count(distinct shop_id) as shop_count
from
	shop_level_roas
where
	month = "2021-04-01" and budget_constrained_shop = 1
	-- and roas_group is not null and gms_share_group_6mo is not null
group by 1,2,3
order by 1,2,3
;

-- impact of backsliding


-- ROAS and GMS group matrix
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month between "2021-04-01" and "2022-03-01"
),base2 as (
select
	roas_group,
	gms_share_group,
	case when seller_tier_new in ("Top Shop","Power Shop") then 1 else 0 end as top_seller_status,
	month,
	count(distinct shop_id) as shop_count,
	avg(total_budget)/avg(budget_month_1)-1 as budget_change
from
	base
where
	month < "2022-04-01"
group by 1,2,3,4
order by 1,2,3,4
)
select
	roas_group,
	gms_share_group,
	top_seller_status,
	avg(budget_change) as avg_budget_change
from
	base2
group by 1,2,3
;

-- what is the impact of creating a ROAS floor of 2 and limiting backsliding?
with base as (
select
	*,
	coalesce(lead(total_budget,1) over(partition by shop_id order by month),0) as budget_month_1,
	coalesce(lead(total_budget,2) over(partition by shop_id order by month),0) as budget_month_2,
	coalesce(lead(total_budget,3) over(partition by shop_id order by month),0) as budget_month_3,
	coalesce(lead(total_budget,4) over(partition by shop_id order by month),0) as budget_month_4,
	coalesce(lead(total_budget,5) over(partition by shop_id order by month),0) as budget_month_5,
	coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,6) over(partition by shop_id order by month),0) as budget_month_6,
	-- coalesce(lead(total_budget,7) over(partition by shop_id order by month),0) as budget_month_7,
	-- coalesce(lead(total_budget,8) over(partition by shop_id order by month),0) as budget_month_8,
	-- coalesce(lead(total_budget,9) over(partition by shop_id order by month),0) as budget_month_9,
	-- coalesce(lead(total_budget,10) over(partition by shop_id order by month),0) as budget_month_10,
	-- coalesce(lead(total_budget,11) over(partition by shop_id order by month),0) as budget_month_11,
	lead(roas_group,1) over(partition by shop_id order by month) as roas_group_month_1,
	lead(roas_group,2) over(partition by shop_id order by month) as roas_group_month_2,
	lead(roas_group,3) over(partition by shop_id order by month) as roas_group_month_3,
	lead(roas_group,4) over(partition by shop_id order by month) as roas_group_month_4,
	lead(roas_group,5) over(partition by shop_id order by month) as roas_group_month_5,
	-- lead(roas_group,6) over(partition by shop_id order by month) as roas_group_month_6,
	-- lead(roas_group,7) over(partition by shop_id order by month) as roas_group_month_7,
	-- lead(roas_group,8) over(partition by shop_id order by month) as roas_group_month_8,
	-- lead(roas_group,9) over(partition by shop_id order by month) as roas_group_month_9,
	-- lead(roas_group,10) over(partition by shop_id order by month) as roas_group_month_10,
	-- lead(roas_group,11) over(partition by shop_id order by month) as roas_group_month_11
	coalesce(lead(total_ea_revenue,1) over(partition by shop_id order by month),0) as ea_revenue_month_1,
	coalesce(lead(total_ea_revenue,2) over(partition by shop_id order by month),0) as ea_revenue_month_2,
	coalesce(lead(total_ea_revenue,3) over(partition by shop_id order by month),0) as ea_revenue_month_3,
	coalesce(lead(total_ea_revenue,4) over(partition by shop_id order by month),0) as ea_revenue_month_4,
	coalesce(lead(total_ea_revenue,5) over(partition by shop_id order by month),0) as ea_revenue_month_5,
	coalesce(lead(total_spend,1) over(partition by shop_id order by month),0) as total_spend_month_1,
	coalesce(lead(total_spend,2) over(partition by shop_id order by month),0) as total_spend_month_2,
	coalesce(lead(total_spend,3) over(partition by shop_id order by month),0) as total_spend_month_3,
	coalesce(lead(total_spend,4) over(partition by shop_id order by month),0) as total_spend_month_4,
	coalesce(lead(total_spend,5) over(partition by shop_id order by month),0) as total_spend_month_5,
	coalesce(lead(total_gms,1) over(partition by shop_id order by month),0) as total_gms_month_1,
	coalesce(lead(total_gms,2) over(partition by shop_id order by month),0) as total_gms_month_2,
	coalesce(lead(total_gms,3) over(partition by shop_id order by month),0) as total_gms_month_3,
	coalesce(lead(total_gms,4) over(partition by shop_id order by month),0) as total_gms_month_4,
	coalesce(lead(total_gms,5) over(partition by shop_id order by month),0) as total_gms_month_5,
	coalesce(lag(total_spend,1) over(partition by shop_id order by month),0) as total_spend_last_month_1,
	coalesce(lag(total_ea_revenue,1) over(partition by shop_id order by month),0) as total_ea_revenue_last_month_1,
	lag(roas_group,1) over(partition by shop_id order by month) as roas_group_last_month_1
from
	`etsy-data-warehouse-dev.pdavidoff.roas_target_analysis`
where
	month >= "2021-04-01"
),shop_level_roas as (
select
	*,
	case 
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 0 and 1 then "1. ROAS 0-1"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 1 and 2 then "2. ROAS 1-2"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 2 and 3 then "3. ROAS 2-3"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 3 and 4 then "4. ROAS 3-4"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 4 and 5 then "5. ROAS 4-5"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 5 and 6 then "6. ROAS 5-6"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) between 6 and 7 then "7. ROAS 6-7"
		when safe_divide(total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5,total_spend+total_spend_month_1+total_spend_month_2+total_spend_month_3+total_spend_month_4+total_spend_month_5) >= 7 then "8. ROAS 7+"
	end as roas_group_6mo,
	case 
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) = 0 then "1. 0% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0 and 0.25 then "2. 0%-20% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.25 and 0.4 then "3. 20%-40% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.4 and 0.6 then "4. 40%-60% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.6 and 0.8 then "5. 60%-80% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) between 0.8 and 0.99 then "6. 80%-100% GMS Share"
		when safe_divide(1,(safe_divide(total_gms+total_gms_month_1+total_gms_month_2+total_gms_month_3+total_gms_month_4+total_gms_month_5,total_ea_revenue+ea_revenue_month_1+ea_revenue_month_2+ea_revenue_month_3+ea_revenue_month_4+ea_revenue_month_5))) > 0.99 then "7. 100% GMS Share"
	end as gms_share_group_6mo
from
	base
),updated_spend as (
select
	*,
	case when roas_group in ("1. ROAS 0-1","2. ROAS 1-2") then ((total_ea_revenue/total_spend)/2)*total_spend else total_spend end as total_spend_roas_2,
	case 
		when roas_group in ("1. ROAS 0-1","2. ROAS 1-2") then total_spend
		when roas_group_last_month_1 = "3. ROAS 2-3" and roas_group < roas_group_last_month_1 then ((total_ea_revenue/total_spend)/2)*total_spend
		when roas_group_last_month_1 = "4. ROAS 3-4" and roas_group < roas_group_last_month_1 then ((total_ea_revenue/total_spend)/3)*total_spend
		when roas_group_last_month_1 = "5. ROAS 4-5" and roas_group < roas_group_last_month_1 then ((total_ea_revenue/total_spend)/4)*total_spend
		when roas_group_last_month_1 = "6. ROAS 5-6" and roas_group < roas_group_last_month_1 then ((total_ea_revenue/total_spend)/5)*total_spend
		when roas_group_last_month_1 = "7. ROAS 6-7" and roas_group < roas_group_last_month_1 then ((total_ea_revenue/total_spend)/6)*total_spend
		when roas_group_last_month_1 = "8. ROAS 7+" and roas_group < roas_group_last_month_1 then ((total_ea_revenue/total_spend)/7)*total_spend
	 else total_spend end as total_spend_roas_backslide
from 
	shop_level_roas
)
select
	roas_group,
	count(distinct shop_id) as shop_count,
	sum(total_budget) as total_budget,
	sum(total_spend) as total_spend,
	sum(total_spend_roas_2) as total_spend_roas_2,
	sum(total_spend_roas_backslide) as total_spend_roas_backslide,
	sum(total_ea_revenue) as total_ea_revenue,
	sum(case when budget_constrained_shop = 1 then total_budget end) as bc_budget,
	sum(case when budget_constrained_shop = 1 then total_spend end) bc_spend,
	sum(case when budget_constrained_shop = 1 then total_spend_roas_2 end) as bc_spend_roas_2,
	sum(case when budget_constrained_shop = 1 then total_spend_roas_backslide end) as bc_spend_roas_backslide,
	sum(case when budget_constrained_shop = 1 then total_ea_revenue end) as bc_revenue
from 
	updated_spend
group by 1
order by 1
;



