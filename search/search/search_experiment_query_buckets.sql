
-- query level data
select
	_date,
	visit_id,
	session_index,
	query,
	has_click,
	has_favorite,
	has_cart,
	attributed_gms,

from
	`etsy-data-warehouse-prod.query_sessions_new`
where
	max_page = 1

select
	*
from
	`etsy-data-warehouse-prod.rollups.search_ads_unified_query_sessions`
limit 5
;

SELECT 
  query_intent,
  impression_type,
  count(*) as query_session_count,
  AVG(has_click) AS click_rate_query_session
FROM `etsy-data-warehouse-prod.rollups.search_ads_unified_query_sessions`
WHERE _date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
  AND page = "search"
GROUP BY 1,2
;

select
	impression_type,
	count(*) as row_count,
	count(distinct visit_id) as visit_count,
	avg(has_click) as click_rate,
	avg(has_cart) as cart_rate,
	avg(has_purchase) as has_purchase
from
	`etsy-data-warehouse-prod.rollups.search_ads_unified_query_sessions`
where
	page = "search"
group by 1
;