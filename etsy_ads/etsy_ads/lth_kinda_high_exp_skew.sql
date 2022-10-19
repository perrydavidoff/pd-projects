create or replace table
  `etsy-data-warehouse-dev.pdavidoff.kinda_high_desktop`
  as (
with base as ( 
select
	(split(visit_id, ".")[ORDINAL(1)]) as browser_id,
	ab_variant,
	min(visit_id) as first_visit,
	min(_date) as first_bucket_date
from
	`etsy-data-warehouse-prod.catapult.ab_tests`
where
	ab_test = "ranking/badx.2022_q1.low_high_ads.search_v2.desktop" and
	_date between "2022-01-19" and "2022-01-31"
group by 1,2
),base2 as (
select
	a.browser_id,
	a.first_bucket_date,
	a.first_visit,
	a.ab_variant,
	-- a.sequence_number,
	count(b.click_key) as clicks,
	sum(b.cost) as cost,
	sum(b.num_orders) as num_orders,
	sum(b.gms_one_day) as attr_gms
from
	base a left join
	`etsy-data-warehouse-prod.rollups.prolist_click_mart` b on a.browser_id = (split(b.visit_id, ".")[ORDINAL(1)]) and b.visit_id >= a.first_visit
group by 1,2,3,4
)
select
	a.*,
	sum(b.orders) as total_orders,
	sum(b.total_gms) as total_gms,
	max(converted) as converted
from
	base2 a left join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.browser_id = b.browser_id and b.visit_id >= a.first_visit and b._date between "2022-01-19" and "2022-01-31"
group by 1,2,3,4,5,6,7,8
)
;


-- overall results
select
	ab_variant,
	count(browser_id) as browser_count,
	sum(cost)/count(browser_id) as prolist_spend,
	sum(clicks)/count(browser_id) as clicks_per_browser,
	count(case when converted = 1 then browser_id end)/count(browser_id) as browser_cr,
	sum(case when total_orders > 0 then total_orders end)/count(case when total_orders > 0 then browser_id end) as ocb
from
	`etsy-data-warehouse-dev.pdavidoff.kinda_high_desktop`
group by 1
order by 1 desc
;

-- filter out early browsers
select
	ab_variant,
	count(browser_id) as browser_count,
	sum(cost)/count(browser_id) as prolist_spend,
	sum(clicks)/count(browser_id) as clicks_per_browser,
	count(case when converted = 1 then browser_id end)/count(browser_id) as browser_cr,
	sum(case when total_orders > 0 then total_orders end)/count(case when total_orders > 0 then browser_id end) as ocb
from
	`etsy-data-warehouse-dev.pdavidoff.kinda_high_desktop`
where
	first_bucket_date >= "2022-01-24"
group by 1
order by 1 desc
;