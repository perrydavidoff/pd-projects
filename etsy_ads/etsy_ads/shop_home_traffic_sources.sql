--Shop Home Web Visits
create or replace table 
    `etsy-data-warehouse-dev.pdavidoff.sh_pages` 
    as (
with all_pages as 
(select visit_id, listing_id, event_type, sequence_number, epoch_ms, ref_tag, 
lag (event_type) over (partition by visit_id order by visit_id, sequence_number) as prev_page,
lag (listing_id) over (partition by visit_id order by visit_id, sequence_number) as prev_listing_id,
lead (sequence_number) over (partition by visit_id order by visit_id, sequence_number) as next_sequence,
lead (event_type) over (partition by visit_id order by visit_id, sequence_number) as next_page,
lead (listing_id) over (partition by visit_id order by visit_id, sequence_number) as next_listing_id,
lead (epoch_ms) over (partition by visit_id order by visit_id, sequence_number) as next_timestamp,
lead (ref_tag) over (partition by visit_id order by visit_id, sequence_number) as next_ref_tag
from `etsy-data-warehouse-prod.weblog.events`
where page_view = 1
)
select * from all_pages where event_type = "shop_home");

select
	*
from
    `etsy-data-warehouse-dev.pdavidoff.sh_pages` 
limit 50;	

create or replace table 
    `etsy-data-warehouse-dev.pdavidoff.shop_home_referrers` 
    as (
with shop_views as (
	select 
		a.visit_id, 
		a.run_date, 		
		-- (regexp_substr(split(lower(url), "shop/", 2), "\w+")) as shop_name, 
		sequence_number, 
		ref_tag,
		platform,
		is_tablet,
		top_channel,
		canonical_region,
		b.user_id,
		buyer_segment,
		-- is_seller,
		converted,
		total_gms
		from `etsy-data-warehouse-prod.weblog.events` a
		join `etsy-data-warehouse-prod.weblog.recent_visits` b using (visit_id)
		left join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` c
    	on b.user_id = c.mapped_user_id
		where event_type = "shop_home" 
		and url is not null 
		and platform in ("desktop", "mobile_web")
		and a.is_preliminary = 0
		and b._date >= current_date - 30
		order by 1,2
	),ranked as (
	select 
		visit_id, 
		min(sequence_number) as first_shop 
	from shop_views 
		group by 1
	)
	select 
		a.*,
		-- d.shop_id,
		-- case when a.user_id = d.user_id then 1 else 0 end as is_own_shop, 
		ifnull(b.prev_page,"landing") as prev_page,
		ifnull(b.next_page,"exit") as next_page,
		b.prev_listing_id,
		b.next_listing_id,
		b.next_ref_tag,
		(b.next_timestamp - b.epoch_ms)/1000 as dwell_seconds
		from shop_views a 
		join `etsy-data-warehouse-dev.pdavidoff.sh_pages` b using (visit_id, sequence_number)
		join ranked c on a.sequence_number = c.first_shop and a.visit_id = c.visit_id
		-- join `etsy-data-warehouse-prod.rollups.seller_basics` d on lower(a.shop_name) = lower(d.shop_name)
	where
		extract(date from timestamp_millis(epoch_ms)) >= current_date - 30
	);

-- previous page
	with ranked as (
  select visit_id, 
  min(sequence_number) as sequence_number 
  from `etsy-data-warehouse-dev.pdavidoff.shop_home_referrers`
  -- where (is_seller = 0 or is_seller is null)
  group by 1),
next_page as (
  select a.*
  from `etsy-data-warehouse-dev.pdavidoff.shop_home_referrers` a 
  join ranked b using (visit_id, sequence_number)),
totals as (
  select 
  count(*) as total_visits,
  sum(total_gms) as total_gms
  from next_page),
main as (
  select prev_page,
  count(*)/total_visits as pct_visits,
  count(*) as visits,
  sum(converted) as converting_visits,
  sum(case when converted = 1 then a.total_gms end)/b.total_gms as pct_gms,
  sum(case when converted = 1 then a.total_gms end) as total_gms
  from next_page a
  cross join totals b
  group by 1, total_visits, b.total_gms
  order by 2 desc)
select prev_page,
  sum(pct_visits) as pct_visits,
  sum(pct_gms) as pct_gms,
  sum(converting_visits)/sum(visits) as cr,
  sum(total_gms) / sum(converting_visits) as acvv
  from main
  where pct_visits >= 0.005
  group by 1
  order by 2 desc;

-- next page
	with ranked as (
  select visit_id, 
  min(sequence_number) as sequence_number 
  from `etsy-data-warehouse-dev.pdavidoff.shop_home_referrers`
  -- where (is_seller = 0 or is_seller is null)
  group by 1),
next_page as (
  select a.*
  from `etsy-data-warehouse-dev.pdavidoff.shop_home_referrers` a 
  join ranked b using (visit_id, sequence_number)),
totals as (
  select 
  count(*) as total_visits,
  sum(total_gms) as total_gms
  from next_page),
main as (
  select next_page,
  count(*)/total_visits as pct_visits,
  count(*) as visits,
  sum(converted) as converting_visits,
  sum(case when converted = 1 then a.total_gms end)/b.total_gms as pct_gms,
  sum(case when converted = 1 then a.total_gms end) as total_gms
  from next_page a
  cross join totals b
  group by 1, total_visits, b.total_gms
  order by 2 desc)
select next_page,
  sum(pct_visits) as pct_visits,
  sum(pct_gms) as pct_gms,
  sum(converting_visits)/sum(visits) as cr,
  sum(total_gms) / sum(converting_visits) as acvv
  from main
  where pct_visits >= 0.005
  group by 1
  order by 2 desc;
