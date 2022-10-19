with base as (
select
	date_trunc(_date,week) as week,
	count(distinct visit_id) as visit_count
from
	`etsy-data-warehouse-prod.weblog.visits`
where
	_date >= "2018-01-01"
),imp_base as (
select
	date_trunc(extract(date from (timestamp_seconds(cast(timestamp as int64))),week) as week,
	count(distinct visit_id) as visits_with_imp,
	count(*) as total_imps,
	count(*)/count(distinct visit_id) as visits_per_visit_with_imp
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	extract(date from (timestamp_seconds(cast(timestamp as int64))) >= "2018-01-01"
group by 1
)
select
	a.week,
	a.visit_count,
	b.visits_with_imp/a.visit_count as pct_visits_with_imp,
	b.visits_with_imp,
	b.total_imps,
	b.visits_per_visit_with_imp
from
	base a join
	imp_base b on a.week = b.week
order by 1
;

-- imps per visit with imp
select
	date_trunc(extract(date from (timestamp_seconds(cast(timestamp as int64))))),week) as week,
	count(case when page_type = 0 then visit_id end)/sum(count(visit_id)) over() as search_imps,
	count(case when page_type = 1 then visit_id end)/sum(count(visit_id)) over() as market_imps,
	count(case when page_type = 2 then visit_id end)/sum(count(visit_id)) over() as cat_imps,
	count(case when page_type in (4,5,6,7,8,9) then visit_id end)/sum(count(visit_id)) over() as listing_imps
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	_PARTITIONDATE >= "2020-02-01"
group by 1
order by 1
;

-- 35% of visits have an ad impression
-- (search, listing page, category, market)
select
	a.platform,
	count(distinct a.visit_id) as visit_count,
	count(distinct b.visit_id) as visit_with_imp_count,
	count(distinct b.visit_id)/count(distinct a.visit_id) as pct_visits_with_imp
from
	`etsy-data-warehouse-prod.weblog.recent_visits` a left join
	`etsy-data-warehouse-prod.weblog.events` b on a.visit_id = b.visit_id and b.event_type = "prolist_imp_full" and b._date >= "2022-03-01"
where
	a._date >= "2022-03-01"
group by 1
;

