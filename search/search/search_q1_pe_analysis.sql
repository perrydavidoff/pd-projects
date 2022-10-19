-- look into experiments that the search ranking and matching teams launched in q1
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
	a.conv_pct_change as banked_cr_change,
	a.winsorized_acbv_pct_change as banked_acbv_change,
	a.gms_ann,
	a.rev_coverage,
	a.prolist_pct_change,
	-- a.gms_coverage,
	-- a.rev_ann,
	layer_start,
	layer_end,
	status,

	-- a.launch_id
from
	`etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` a left join
	`etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b on a.experiment_name = b.name left join
	`etsy-data-warehouse-prod.exp_on_key_metrics` c on 
where
	subteam like "%Search%" and end_date between "2022-01-01" and "2022-03-31"
	-- and prolist_pct_change > 0 and 
	-- status like "%Ramped Up%"
order by end_date
;

with max_date as (
select
	experiment_id,
	bound_start_date,
	variant_name,
	min(desc_run_date) as max_date_bound
from
	`etsy-data-warehouse-prod.catapult.exp_on_key_metrics`
where
	segmentation = "any" and segment = "all"
group by 1,2,3
),one_row_per_exp_bound_variant as (
select
	a.experiment_id,
	date(timestamp_seconds(a.bound_start_date)) as bound_start_date,
	desc_run_date,
	a.variant_name,
	desc_bound_num,
	conv_rate_pct_change_cuped,
	conv_rate_is_powered_cuped,
	a.winsorized_acbv_pct_change,
	winsorized_acbv_p_value,
	winsorized_acbv_pct_change_cuped,
	winsorized_acbv_p_value_cuped,
	a.on_prolist_pct_change,
	on_prolist_pct_p_value,
	on_purchase_freq_pct_change,
	on_purchase_freq_p_value,
	on_purchase_freq_pct_change_cuped,
	on_purchase_freq_p_value_cuped,
	on_ocb_pct_change,
	on_ocb_p_value,
	on_avg_order_value_pct_change,
	on_avg_order_value_p_value
from
	`etsy-data-warehouse-prod.catapult.exp_on_key_metrics` a join
	max_date b on a.experiment_id = b.experiment_id and a.desc_run_date = b.max_date_bound and a.bound_start_date = b.bound_start_date and a.variant_name = b.variant_name
where
	segmentation = "any" and segment = "all"
)
select
	distinct
	a.experiment_id,
	b.name,
	b.team,
	c.status,
	a.variant_name,
	start_date,
	end_date,
	-- desc_run_date,
	-- desc_bound_num,
	conv_rate_pct_change_cuped,
	conv_rate_is_powered_cuped,
	a.winsorized_acbv_pct_change,
	winsorized_acbv_p_value,
	winsorized_acbv_pct_change_cuped,
	winsorized_acbv_p_value_cuped,
	a.on_prolist_pct_change,
	on_prolist_pct_p_value,
	on_purchase_freq_pct_change,
	on_purchase_freq_p_value,
	on_purchase_freq_pct_change_cuped,
	on_purchase_freq_p_value_cuped,
	on_ocb_pct_change,
	on_ocb_p_value,
	on_avg_order_value_pct_change,
	on_avg_order_value_p_value,
	c.gms_coverage,
	c.conv_pct_change as banked_cr_change,
	log_acbv_pct_change,
	c.kpi_initiative_name,
	c.kpi_initiative_value,
	c.kpi_initiative_coverage,
	c.start_layer
from 
	one_row_per_exp_bound_variant a join
	`etsy-data-warehouse-prod.catapult.exp_summary` b on a.experiment_id = b.experiment_id join
	`etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` c on b.experiment_id = c.launch_id and bound_start_date = c.start_date and a.variant_name = c.variant and c.status like "%Ramp%" and c.status like "%Up%"
where
	team in ("Search Ranking","Search Matching") and end_date between "2022-01-01" and "2022-03-31"
order by experiment_id desc
limit 50
;


-- daily search PE
-- with max_date as (
-- select
-- 	experiment_id,
-- 	bound_start_date,
-- 	variant_name,
-- 	min(desc_run_date) as max_date_bound
-- from
-- 	`etsy-data-warehouse-prod.catapult.exp_on_key_metrics`
-- where
-- 	segmentation = "any" and segment = "all"
-- group by 1,2,3
-- ),one_row_per_exp_bound_variant as (
with base as (
select
	a.experiment_id,
	date(timestamp_seconds(a.bound_start_date)) as bound_start_date,
	desc_run_date,
	a.variant_name,
	desc_bound_num,
	conv_rate_pct_change_cuped,
	conv_rate_is_powered_cuped,
	a.winsorized_acbv_pct_change,
	winsorized_acbv_p_value,
	winsorized_acbv_pct_change_cuped,
	winsorized_acbv_p_value_cuped,
	a.on_prolist_pct_change,
	on_prolist_pct_p_value,
	on_purchase_freq_pct_change,
	on_purchase_freq_p_value,
	on_purchase_freq_pct_change_cuped,
	on_purchase_freq_p_value_cuped,
	on_ocb_pct_change,
	on_ocb_p_value,
	on_avg_order_value_pct_change,
	on_avg_order_value_p_value
from
	`etsy-data-warehouse-prod.catapult.exp_on_key_metrics` a
where
	segmentation = "any" and segment = "all"
)
select
	distinct
	a.experiment_id,
	b.name,
	b.team,
	a.variant_name,
	bound_start_date,
	-- start_date,
	-- end_date,
	desc_run_date,
	conv_rate_pct_change_cuped,
	conv_rate_is_powered_cuped,
	a.winsorized_acbv_pct_change,
	winsorized_acbv_p_value,
	winsorized_acbv_pct_change_cuped,
	winsorized_acbv_p_value_cuped,
	a.on_prolist_pct_change,
	on_prolist_pct_p_value,
	on_purchase_freq_pct_change,
	on_purchase_freq_p_value,
	on_purchase_freq_pct_change_cuped,
	on_purchase_freq_p_value_cuped,
	on_ocb_pct_change,
	on_ocb_p_value,
	on_avg_order_value_pct_change,
	on_avg_order_value_p_value
from 
	base a join
	`etsy-data-warehouse-prod.catapult.exp_summary` b on a.experiment_id = b.experiment_id and ab_test = "team_experiments.2022_q1.search.manual"
order by experiment_id desc,bound_start_date,desc_run_date desc
limit 50
;

`etsy-data-warehouse-prod.catapult.exp_summary`