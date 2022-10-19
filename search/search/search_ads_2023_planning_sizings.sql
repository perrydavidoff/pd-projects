-- search experiment data
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
	a.log_acbv_pct_change,
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
	subteam like "%Search%" or subteam = "Marketplace Optimization" and extract(year from end_date) >= 2019
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

-- try with new table
select
	start_date,
	end_date,
	extract(year from end_date) as end_year,
	initiative,
	subteam,
	experiment_name,
	variant,
	gms_coverage,
	conv_pct_change,
	winsorized_acbv_pct_change,
	log_acbv_pct_change,
	rev_coverage,
	prolist_pct_change,
	status,
	cr_adjustment,
	acbv_adjustment,
	discounting_model_cr_lift,
	discounting_model_acbv_lift,
	discounted_gms_lift,
	rev_lift,
	global_conv_pct_change,
	global_winsorized_acbv_pct_change
from
	`etsy-data-warehouse-prod.etsy_atlas.catapult_experiment_reports_with_impact`
where
	subteam like "%Search" or subteam = "Marketplace Optimization" and bug = 0
;
