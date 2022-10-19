-- sizing the opportunity around optimizing for GMS in the objective function
-- use just page 1 becauase 95% of purchases come from that page
with base as (
select
	distinct
	visit_id,
	listing_id,
	page
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions`
where
	_date >= current_date - 7 and page_no = 1
order by listing_id
),base2 as (
select
	a.*,
	price_usd
from
	base a join
	`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = b.listing_id
)
select
	avg(price_usd) as avg_price
	-- percentile_cont(price_usd,0.5) over() as median_price
from
	base2
;


-- purchase rate and query median price
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_level_imp_cr`
  as (
with distinct_listings as (
select
	distinct
	visit_id,
	query,
	listing_id
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions`
where
	_date >= current_date - 30 and page_no = 1
order by listing_id
),listing_price_join as (
select
	a.visit_id,
	a.query,
	a.listing_id,
	b.price_usd/100 as price_usd
from
	distinct_listings a join
	`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = b.listing_id
)
select
	distinct
	-- visit_id,
	query,
	-- listing_id,
	percentile_cont(price_usd,0.5) over(partition by query) as median_query_price	
from
	listing_price_join
order by query
)
;

-- join with purchase rates
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_level_price_performance`
  as (
with purchase_base as (
select
	query,
	count(*) as query_session,
	sum(has_purchase)/count(*) as search_cr,
	sum(has_purchase) as total_purchases
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date >= current_date - 30
group by 1
order by query 
)
select
	a.*,
	b.median_query_price	
from
	purchase_base a join
	`etsy-data-warehouse-dev.pdavidoff.query_level_imp_cr` b on a.query = b.query
)
;



with base as (
select
	receipt_id,
	sum(quantity) as total_quantity,
	sum(usd_subtotal_price) as gms,
	sum(usd_subtotal_price)/sum(quantity) as aiv,
	sum(quantity)/count(distinct receipt_id) as items_per_order	
from
	`etsy-data-warehouse-prod.transaction_mart.all_transactions`
where
	current_date - 30 <= extract(date from creation_tsz)
group by 1
),base2 as (
select
	*,
	ntile(10) over(order by aiv) as aiv_decile
from
	base
)
select
	aiv_decile,
	count(distinct receipt_id) as receipt_count,
	avg(aiv) as avg_aiv,
	avg(items_per_order) as items_per_order,
	avg(gms) as avg_gms
from
	base2
group by 1
order by 1
;



-- experiment summary for search

-- experiment summary since 2019
with base as (
select
	-- a.launch_date,
	start_date,
	end_date,
	extract(year from end_date) as end_year,
	a.initiative,
	a.subteam,
	a.experiment_name,
	-- a.start_date,
	-- a.end_date,
	a.gms_coverage,
	a.conv_pct_change,
	a.winsorized_acbv_pct_change,
	a.rev_coverage,
	a.prolist_pct_change,
	case when layer_start is null then 0 else layer_start end as layer_start,
	case when layer_end is null then 100 else layer_end end as layer_end,
	status
	-- a.launch_id
from
	`etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` a left join
	`etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b on a.experiment_name = b.name
where
	subteam like "%Search%" and extract(year from end_date) >= 2019
	-- and prolist_pct_change > 0 and 
	-- status like "%Ramped Up%"
order by end_date
)
select
	*,
	((gms_coverage*conv_pct_change)/((layer_end-layer_start)/100)) as cvg_adj_cr_impact,
	((gms_coverage*winsorized_acbv_pct_change)/((layer_end-layer_start)/100)) as cvg_adj_acbv_impact,
	((rev_coverage*prolist_pct_change)/((layer_end-layer_start)/100)) as cvg_adj_rev_impact
from
	base
order by end_date
;

-- performance tests not run by the search team --> DEP
select
	-- a.launch_date,
	start_date,
	end_date,
	extract(year from end_date) as end_year,
	a.initiative,
	a.subteam,
	a.experiment_name,
	-- a.start_date,
	-- a.end_date,
	a.gms_coverage,
	a.conv_pct_change,
	a.winsorized_acbv_pct_change,
	a.log_acbv_pct_change,
	a.gms_ann,
	a.rev_coverage,
	a.prolist_pct_change,
	-- a.gms_coverage,
	-- a.rev_ann,
	case when layer_start is null then 0 else layer_start end as layer_start,
	case when layer_end is null then 100 else layer_end end as layer_end,
	status
	-- a.launch_id
from
	`etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` a left join
	`etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b on a.experiment_name = b.name
where
	(experiment_name like "%signal%" or experiment_name like "%nudge%" or experiment_name like "%badge%"
	or experiment_name like "%alert%") and extract(year from end_date) >= 2019
	-- and prolist_pct_change > 0 and 
	-- status like "%Ramped Up%"
order by end_date
;

