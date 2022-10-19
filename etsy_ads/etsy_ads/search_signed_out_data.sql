create or replace table
  `etsy-data-warehouse-dev.pdavidoff.first_listing_view`
  as (
with base as (
select
  visit_id,
  listing_id,
  timestamp_millis(epoch_ms) as listing_time,
  rank() over (partition by visit_id order by timestamp_millis(epoch_ms)) as rank
from 
  `etsy-data-warehouse-prod.analytics.listing_views`
where
  _date >= "2021-01-01"
qualify rank = 1
),first_search as (
select
  visit_id,
  timestamp_millis(start_epoch_ms) as search_time,
  rank() over(partition by visit_id order by timestamp_millis(start_epoch_ms)) as search_rank
from
  `etsy-data-warehouse-prod.search.query_sessions_new`
where
  _date >= "2021-01-01"
qualify search_rank = 1
),join_searches as (
select
  _date,
  a.visit_id,
  timestamp_millis(a.start_epoch_ms) as first_search_time,
  max_page,
  has_cart,
  has_purchase,
  attributed_gms,
  b.listing_time as first_listing_view,
  c.search_time as followup_search
from
  `etsy-data-warehouse-prod.search.query_sessions_new` a left join
  base b on a.visit_id = b.visit_id left join
  first_search c on a.visit_id = c.visit_id
where
  _date >= "2021-01-01"
)
select
  a.*,
  b.user_id,
  case 
    when b.user_id is null then "Signed Out"
    when c.buyer_segment is null then "New"
    else c.buyer_segment
  end as buyer_segment
from
  join_searches a join 
  `etsy-data-warehouse-prod.weblog.visits` b on a.visit_id = b.visit_id and b._date >= "2021-01-01"left join 
  `etsy-data-warehouse-prod.catapult.catapult_daily_buyer_segments` c on b.user_id = c.user_id and b._date = c._date
)
;

-- what percent of searches have personalization data in visit?
select
  buyer_segment,
  count(visit_id) as visit_share,
  count(distinct visit_id) as distinct_visits,
  count(case when first_listing_view < first_search_time or followup_search > first_search_time then visit_id end)/count(visit_id) as personalization_share
from
  `etsy-data-warehouse-dev.pdavidoff.first_listing_view`
group by 1
order by 2 desc
;

select
  visit_id,
  ref_tag,
  cast(SPLIT(ref_tag,"-")[SAFE_OFFSET(1)] as int64) as page_no,
  cast(SPLIT(ref_tag,"-")[SAFE_OFFSET(2)] as int64) as position
from
  `etsy-data-warehouse-prod.analytics.listing_views`
where
  _date = "2022-04-10" and platform in ("destkop","mobile_web")
  and referring_page_event = "search"
limit 50
;


-- purchase rate by perso type and signed out/new buyers only
select
  date_trunc(_date,month) as month,
  avg(case when first_search_time >= first_listing_view and followup_search = first_search_time then has_purchase end) as non_perso_purch_rate,
  avg(case when first_listing_view < first_search_time or followup_search > first_search_time then has_purchase end) as perso_purch_rate
from
  `etsy-data-warehouse-dev.pdavidoff.first_listing_view`  
where
  buyer_segment in ("Signted Out","New")
group by 1
order by 1
;

select
  date_trunc(_date,month) as month,
  count(case when first_search_time >= first_listing_view and followup_search = first_search_time and max_page = 1 then visit_id end)/count(case when first_search_time >= first_listing_view and followup_search = first_search_time then visit_id end) as non_perso_first_page_rate,
  count(case when (first_listing_view < first_search_time or followup_search > first_search_time) and max_page = 1 then has_purchase end)/count(case when (first_listing_view < first_search_time or followup_search > first_search_time) then visit_id end) as perso_first_page_rate
from
  `etsy-data-warehouse-dev.pdavidoff.first_listing_view`
where
  buyer_segment in ("Signed Out","New") and has_purchase = 1
group by 1
order by 1
;

select
  *
from
  `etsy-data-warehouse-dev.pdavidoff.first_listing_view`
limit 50
;

select
  _date,
  count(case when max_page = 1 then visit_id end)/count(visit_id)
from
