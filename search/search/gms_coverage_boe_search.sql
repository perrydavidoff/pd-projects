with base as (
	select
		distinct
		visit_id,
		1 as search_visit
	from
		`etsy-data-warehouse-prod.weblog.events`
	where
		event_type = "search"
),visits as (
select
	platform,
	a.visit_id,
	a.total_gms,
	b.search_visit
from
	`etsy-data-warehouse-prod.weblog.recent_visits` a left join
	base b on a.visit_id = b.visit_id
where
	_date >= current_date - 30
)
select
	count(distinct case when platform = "boe" and search_visit = 1 then visit_id end)/count(distinct visit_id) as visit_share,
	sum(case when platform = "boe" and search_visit = 1 then total_gms end)/sum(total_gms) as gms_cvg
from
	visits
;


