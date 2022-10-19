-- what share of advertised listings flag NKT?
with advertised_listings as (
  select
    a.shop_id,
    b.listing_id,
    is_paused
  from
    `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a join
    `etsy-data-warehouse-prod.rollups.active_listing_basics` b on a.shop_id = b.shop_id join
    `etsy-data-warehouse-prod.etsy_shard.prolist_listing` c on b.shop_id = c.shop_id and b.listing_id = c.listing_id
  where
    date = "2022-09-14"
),nkt_join as (
select
  a.*,
  min(is_relevant) as any_nkt_queries,
  count(distinct case when is_relevant = 0 then query end) as distinct_nkt_queries
from
  advertised_listings a left join
  `etsy-data-warehouse-prod.etsy_shard.prolist_listing_query` b on a.listing_id = b.listing_id
group by 1,2,3
)
select
  count(distinct shop_id) as active_shops,
  count(distinct listing_id) as active_listings,
  count(distinct case when is_paused = 0 then listing_id end) as not_paused_listings,
  count(distinct case when is_paused = 0 and any_nkt_queries = 0 then listing_id end) as nkt_listings,
  sum(case when is_paused = 0 and any_nkt_queries = 0 then distinct_nkt_queries end)/count(distinct case when is_paused = 0 and any_nkt_queries = 0 then listing_id end) as nkt_per_listing
from
  nkt_join
;

with impressions as (
  SELECT 
  case
  when activeAdsAbVariants like "%ads.sadx.negative_keyword_targeting:on%" then "on"
  else "off" end as variant
  ,
  visit_id,
  shop_id,
  listing_id,
  query,
  purchase,
  click,
  if(click = 1, cost, 0) as cost 
  FROM `etsy-prolist-etl-prod.prolist.attributed_impressions` WHERE DATE(_PARTITIONTIME) >= "2022-08-26"
  and activeAdsAbVariants like "%ads.sadx.negative_keyword_targeting%"
  and page_type = 0
),
queries as (
  select query
  from `etsy-data-warehouse-prod.etsy_shard.prolist_listing_query` where is_relevant = 0 group by 1
)

select
case when a.variant = "on" then "on"
when a.variant = "off" then "off"
else "other" end as filtered,
count(distinct visit_id) as visit_count,
count(*) as impressions,
sum(click) as clicks,
sum(purchase) as purchases,
sum(click) / count(*) * 100 as ctr,
sum(purchase) / sum(click) * 100 as cvr,
ROUND(sum(cost) / 100) as revenue,
sum(cost) / sum(click) as cpc
from impressions a
join queries b on a.query = b.query
group by 1
;