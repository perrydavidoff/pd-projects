with base as (
select
	-- visit_id,
	a.query as source_query,
	b.query as target_query,
	count(*) as count,
	count(*)/sum(count(*)) over() as weight
from
	`etsy-data-warehouse-prod.search.query_sessions_new` a left join
	`etsy-data-warehouse-prod.search.query_sessions_new` b on a.visit_id = b.visit_id and b._date = "2022-01-08" and a.query != b.query
where
	a._date = "2022-01-08"
group by 1,2
order by 3 desc
limit 50;
)
select
	source,
	target,
	count,
	"undirected" type,
from
	select

with base as (
select
	visit_id,
	query,
	row_number() over(partition by visit_id) as visit_row,
	row_number() over() as visit_query_row
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date = "2021-01-08"
)
select
	visit_id,
	array_agg(query order by query) as query_agg
from
	base
group by 1
order by 1
limit 50;

SELECT Source, Target, Count RawCount, "Undirected" Type, ( Count/SUM(Count) OVER () ) Weight FROM (
SELECT a.entity Source, b.entity Target, COUNT(*) as Count
FROM (
 (SELECT url, entities.name entity FROM `gdelt-bq.gdeltv2.geg_gcnlapi`, unnest(entities) entities where entities.mid is not null and date >= "2019-02-05 00:00:00" AND date < "2019-02-06 00:00:00")
) a
JOIN (
 (SELECT url, entities.name entity FROM `gdelt-bq.gdeltv2.geg_gcnlapi`, unnest(entities) entities where entities.mid is not null and date >= "2019-02-05 00:00:00" AND date < "2019-02-06 00:00:00")
) b
ON a.url=b.url
WHERE a.entity<b.entity
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 1500
)
order by Count Desc
;

SELECT 
	url, 
	entities.name as entity
FROM 
	`gdelt-bq.gdeltv2.geg_gcnlapi`, 
	unnest(entities) entities where entities.mid is not null and date >= "2019-02-05 00:00:00" AND date < "2019-02-06 00:00:00"
limit 50
;
	, unnest(entities) entities where entities.mid is not null and date >= "2019-02-05 00:00:00" AND date < "2019-02-06 00:00:00"
limit 50;

select
	count(case when query_count = 1 then visit_id end)/count(visit_id)
from
	base
;