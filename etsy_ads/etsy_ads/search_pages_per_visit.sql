select
	date_trunc(_date,month) as week,
	count(visit_id) as query_sessions,
	count(distinct visit_id) as visit_count,
	sum(max_page)/count(visit_id) as pages_per_session,
	sum(max_page)/count(distinct visit_id) as pages_per_visit	
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date >= "2020-01-01"
group by 1
order by 1
;
