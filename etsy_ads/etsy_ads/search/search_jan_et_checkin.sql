-- search ET check-in analysis for Jan, 2022
-- goals: outline the GMS and revenue opportunity for the matching team
-- how has coverage changed over time?
-- looks like the share of reults with < one page decreased from 14% in feb to 5% currently.
select
	date_trunc(_date,month) as month,
	count(case when min_total_results = 0 then visit_id end)/count(visit_id) as no_result_query,
	count(case when min_total_results < 48 then visit_id end)/count(visit_id) as one_page,
	count(case when min_total_results < 96 then visit_id end)/count(visit_id) as two_page,
	count(case when min_total_results < 144 then visit_id end)/count(visit_id) as three_page,
	count(case when has_click = 1 then visit_id end)/count(visit_id) as ctr,
	count(case when has_purchase = 1 then visit_id end)/count(visit_id) as purchase_rate
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date >= "2020-01-01"
group by 1
order by 1
;

-- what happens when max page matches page of results?
-- dead ends have a significant negative impact on CR
with low_result_queries as (
select
	query,
	case 
		when avg(min_total_results) = 0 then 0
		when avg(min_total_results) <= 48 then 1
		when avg(min_total_results) between 49 and 96 then 2
		when avg(min_total_results) between 97 and 144 then 3
		when avg(min_total_results) between 145 and 192 then 4
		when avg(min_total_results) between 193 and 240 then 5
		when avg(min_total_results) between 241 and 288 then 6
		else 7
	end as results_page,
	avg(min_total_results) as avg_min_results
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date between "2022-01-01" and "2022-01-14"
group by 1
-- zero result queries
-- less than 1 page
-- more than 1 page
),max_page as (
select
	a.query,
	a.visit_id,
	a.max_page,
	a.has_click,
	a.has_purchase,
	a.attributed_gms,
	b.converted as visit_level_convert
from
	`etsy-data-warehouse-prod.search.query_sessions_new` a join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date between "2022-01-01" and "2022-01-14"
where
	a._date between "2022-01-01" and "2022-01-14"
),page_perf as (
select
	a.query,
	a.results_page,
	case when b.max_page >= a.results_page then 1 else 0 end as max_page,
	case when results_page - max_page = 1 then 1 else 0 end as next_page_results,
	count(case when has_click = 1 then visit_id end) as clicks,
	count(visit_id) as visits,
	count(case when has_purchase = 1 then visit_id end) as search_attr_purchase,
	count(case when visit_level_convert = 1 then visit_id end) as visit_level_purchases	
from
	low_result_queries a join
	max_page b on a.query = b.query
group by 1,2,3,4
)
select
	results_page,
	sum(visits)/sum(sum(visits)) over() as visit_share,
	sum(visits) as total_query_sessions,
	-- sum(case when max_page = 0 then visits end)/sum(sum(case when max_page = 0 then visits end)) over() as no_max_visit_share,
	-- sum(case when max_page = 1 then visits end)/sum(sum(case when max_page = 1 then visits end)) over() as max_visit_share,
	sum(case when max_page = 0 then visits end) as no_max_page_visits,
	sum(case when max_page = 0 then clicks end)/sum(case when max_page = 0 then visits end) as no_max_page_click_rate,
	sum(case when max_page = 0 then search_attr_purchase end)/sum(case when max_page = 0 then visits end) as no_max_page_search_cr,
	sum(case when max_page = 0 then visit_level_purchases end)/sum(case when max_page = 0 then visits end) as no_max_page_visit_cr,
	sum(case when max_page = 1 then visits end) as max_page_visits,
	sum(case when max_page = 1 then clicks end)/sum(case when max_page = 1 then visits end) as max_page_click_rate,
	sum(case when max_page = 1 then search_attr_purchase end)/sum(case when max_page = 1 then visits end) as max_page_search_cr,
	sum(case when max_page = 1 then visit_level_purchases end)/sum(case when max_page = 1 then visits end) as max_page_visit_cr,
	sum(case when max_page = 0 and next_page_results = 1 then clicks end)/sum(case when max_page = 0 and next_page_results = 1 then visits end) as next_page_ctr,	
	sum(case when max_page = 0 and next_page_results = 1 then search_attr_purchase end)/sum(case when max_page = 0 and next_page_results = 1 then visits end) as next_page_search_cr,
	sum(case when max_page = 0 and next_page_results = 1 then visit_level_purchases end)/sum(case when max_page = 0 and next_page_results = 1 then visits end) as next_page_visit_cr
from
	page_perf
group by 1
order by 1
;

-- what happens when max page matches page of results
with low_result_queries as (
select
	query,
	case 
		when avg(min_total_results) = 0 then 0
		when avg(min_total_results) <= 48 then 1
		when avg(min_total_results) between 49 and 96 then 2
		when avg(min_total_results) between 97 and 144 then 3
		when avg(min_total_results) between 145 and 192 then 4
		when avg(min_total_results) between 193 and 240 then 5
		when avg(min_total_results) between 241 and 288 then 6
		else 7
	end as results_page,
	avg(min_total_results) as avg_min_results
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date between "2022-01-01" and "2022-01-14"
group by 1
-- zero result queries
-- less than 1 page
-- more than 1 page
),max_page as (
select
	a.query,
	a.visit_id,
	a.max_page as buyer_page,
	a.has_click,
	a.has_purchase,
	a.attributed_gms,
	b.converted as visit_level_convert
from
	`etsy-data-warehouse-prod.search.query_sessions_new` a join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date between "2022-01-01" and "2022-01-14"
where
	a._date between "2022-01-01" and "2022-01-14"
)
select
	results_page,
	case when buyer_page >= results_page then results_page else buyer_page end as buyer_page,
	count(case when has_click = 1 then visit_id end) as clicks,
	count(visit_id) as visits,
	count(case when has_purchase = 1 then visit_id end) as search_attr_purchase,
	count(case when visit_level_convert = 1 then visit_id end) as visit_level_purchases	
from
	low_result_queries a join
	max_page b on a.query = b.query
group by 1,2
order by 1,2
;

-- how often buyers go to different pages
with base as (
select
	visit_id,
	query,
	max_page,
	has_click,
	has_purchase,
	attributed_gms
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date between "2022-01-01" and "2022-01-16"
)
select
	case when max_page > 7 then 7 else max_page end as max_page,
	count(*)/sum(count(*)) over() as page_share,
	count(case when has_click = 1 then visit_id end)/count(visit_id) as ctr,
	count(case when has_purchase = 1 then visit_id end)/count(visit_id) as purchase_rate,
	sum(case when has_purchase = 1 then attributed_gms end)/count(case when has_purchase = 1 then visit_id end) as gms_per_purchase
from
	base
group by 1
order by 1
;


-- GMS coverage for buyers with a search
with search_base as (
select
	a.visit_id,
	max(case when b.visit_id is not null then 1 else 0 end) as search_visit,
	max(total_gms) as visit_gms
from
	`etsy-data-warehouse-prod.weblog.recent_visits` a left join
	`etsy-data-warehouse-prod.weblog.events` b on a.visit_id = b.visit_id and b.event_type = "market" and b._date between "2022-01-01" and "2022-01-18"
where
	a._date between "2022-01-01" and "2022-01-18"
group by 1
)
select
	sum(case when search_visit = 1 then visit_gms end)/sum(visit_gms) as search_share
from
	search_base
;


-- experiment summary since 2019
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
	layer_start,
	layer_end,
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
;


-- experiment history for the search team
-- another view for 2017
select
	distinct
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
	layer_start,
	layer_end,
	status
	-- d.variant_name,
	-- d.variant_pct_change as mean_click_pct_change,
	-- d.variant_p_value as mean_click_p_value
	-- a.launch_id
from
	`etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` a left join
	`etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b on a.experiment_name = b.name left join
	`etsy-data-warehouse-prod.catapult.catapult_metrics_results` c on b.config_flag = c.ab_test left join
	`etsy-data-warehouse-prod.catapult.exp_oth_anyall_last` d on a.variant = d.variant_name and c.experiment_id = d.experiment_id and a.end_date = extract(date from timestamp_seconds(bound_last_date)) and d.metric = "Mean organic_search_click"
where
	subteam like "%Search%" and extract(year from end_date) >= 2017 and
	(is_long_term_holdout = 0 or is_long_term_holdout is null)
	-- and prolist_pct_change > 0 and 
	-- status like "%Ramped Up%"
order by experiment_name
;


-- what is visit level conversion rate for buyers who have a search?
select
	count(distinct case when b.converted = 1 then a.visit_id end)/count(distinct a.visit_id) as visit_cr
from
	`etsy-data-warehouse-prod.weblog.events` a join
	`etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date >= "2022-01-01"
where
	event_type = "search" and a._date >= "2022-01-01"
;


-- distribution of results for search queries
with base as (
select
	visit_id,
	query,
	min_total_results,
	max_total_results,
	max_page,
	ntile(10) over(order by min_total_results) as results_dist	
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date = "2022-01-16"
)
select
	results_dist,
	avg(min_total_results) as min_total_results
from
	base
group by 1
order by 1
;


-- how much LRQ turnover is there annually?
with base_2018 as (
select
	distinct
	query,
	sum(attributed_gms) as attributed_gms
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	extract(year from _date) = 2018 and max_total_results <= 144
group by 1
),base_2019 as (
select
	distinct
	query,
	sum(attributed_gms) as attributed_gms
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	extract(year from _date) = 2019 and max_total_results <= 144
group by 1
),base_2020 as (
select
	distinct
	query,
	sum(attributed_gms) as attributed_gms
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	extract(year from _date) = 2020 and max_total_results <= 144
group by 1
),base_2021 as (
select
	distinct
	query,
	sum(attributed_gms) as attributed_gms
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	extract(year from _date) = 2021 and max_total_results <= 144
group by 1
)
select
	count(case when b.query is null then a.query end)/count(a.query) as lrq_turnover,
	sum(case when b.query is null then a.attributed_gms end)/sum(a.attributed_gms) as gms_turnover,
	count(case when c.query is null then b.query end)/count(b.query) as lrq_turnover_2y,
	sum(case when c.query is null then b.attributed_gms end)/sum(b.attributed_gms) as gms_turnover_2y,
	count(case when d.query is null then c.query end)/count(c.query) as lrq_turnover_3y,
	sum(case when d.query is null then c.attributed_gms end)/sum(c.attributed_gms) as gms_turnover_3y
from
	base_2021 a left join
	base_2020 b on a.query = b.query left join
	base_2019 c on b.query = c.query left join
	base_2018 d on c.query = d.query
;


