-- Pacing v3 Experiment 10/5 - 10/26
-- this pacing test only paced when a seller had 100% utilization the day before and was on a query
-- that had sufficient ads to replace their listing.
with clicks as (
    select 
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        count(*) as clicks,
        sum(cost/100) as spend,
    from `etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <=
        date_sub(current_date(),interval 2 day)
    group by 1
),
ads_conv as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        count(*) as conv_clicks, 
        sum(revenue/100) as ads_gms
    from `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
        and date(timestamp(cast(timestamp_seconds(purchase_date) as datetime), "UTC"), "America/New_York")  
            <= date_sub(current_date(),interval 2 day)
    group by 1
),
impressions as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        sum(impression_count) as impressions
    from `etsy-data-warehouse-prod.etsy_shard.shop_stats_prolist_snapshot_daily`
    where date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
        and date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), "UTC"), "America/New_York") <= date_sub(current_date(),interval 2 day)
    group by 1
), browsers_temp as (
select
	distinct
	visit_id,
	browser_id,
	date(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") as date
FROM
  `etsy-visit-pipe-prod.canonical.visits` as a join
  UNNEST(a.events.events_tuple) as b on b.event_type = "prolist_imp_full"
where 
	a._date between "2021-10-04" and "2021-10-26"
	-- between "2021-10-04" and "2021-10-27"
),browsers as (
select
    case when mod(extract(day from date), 2) != 0 then "on" else "off" end as variant,
    count(*) as visits,
    count(distinct browser_id) as browsers
from
	browsers_temp
where 
	date >= date("2021-10-04") 
	and date <= date("2021-10-25")
group by 1
)
select
    a.variant,
    d.browsers,
    d.visits,
    c.impressions,
    a.clicks,
    b.conv_clicks,
    a.spend,
    b.ads_gms,
    a.spend / d.browsers as spend_per_browser,
    c.impressions / d.browsers as impr_per_browser,
    a.spend / d.visits as spend_per_visit,
    c.impressions/ d.visits as impr_per_visit,
    a.clicks / c.impressions as ctr,
    a.spend / a.clicks as cpc,
    b.conv_clicks / a.clicks as pccr,
    b.ads_gms / (c.impressions / 1000) as gpm,
    b.ads_gms / a.spend as roas
from clicks a
full outer join ads_conv b
    using (variant)
full outer join impressions c
    using (variant)
full outer join browsers d
    using (variant)
order by 1
;

-- create or replace table
-- identify sellers who were paced.
-- this dataset outputs sellers on the day they were paced, and the day before they were paced
-- identify sellers who were paced. 
-- this dataset will output sellers on the day they were paced and the day before they were paced
create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` 
	as (
with shop_days as (
    select 
        shop_id,
         date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") as date,
        max(click_date) as last_click_timestamp,
        EXTRACT(HOUR FROM datetime(timestamp(cast(timestamp_seconds(max(click_date)) as datetime), "UTC"), "America/New_York")) as last_click_hour,
        extract(date from datetime(timestamp(cast(timestamp_seconds(max(click_date)) as datetime), "UTC"), "America/New_York")) as last_click_dt,
        max(budget) as budget, -- in cents
        sum(cost)/max(budget) * 1.0 as budget_utilization
    from `etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
    where
        date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") between "2021-10-03" and "2021-10-25"
    group by 1,2
),paced_group as (
    select 
        *,
        date_add(date(timestamp(cast(timestamp_seconds(last_click_timestamp) as datetime),"UTC"),"America/New_York"), interval 2 day) as paced_day,
        date_add(date(timestamp(cast(timestamp_seconds(last_click_timestamp) as datetime),"UTC"),"America/New_York"), interval 1 day) as non_paced_day
    from 
        shop_days 
    where
        budget_utilization = 1
        and budget >= 200
        and last_click_hour < 20
),pacing_days as (
select 
    a.shop_id,
    a.last_click_timestamp,
    a.last_click_hour,
    a.last_click_dt,
    a.budget,
    a.budget_utilization,
    case when mod(extract(day from paced_day), 2) != 0 then "on" else "off" end as variant_day,
    extract(date from b.date) as experiment_date,
    a.paced_day,
    a.non_paced_day
from 
    paced_group a cross join 
    `etsy-data-warehouse-prod.public.calendar_dates` b
where
    paced_day between "2021-10-04" and "2021-10-25" and
    (extract(date from b.date) = paced_day or extract(date from b.date) = non_paced_day) -- creating two rows for every shop, one paced, one not paced
)
    select
        case when variant_day = "on" and experiment_date = paced_day then "on" else "off" end as ab_variant,
        variant_day,
        shop_id,
        budget,
        budget_utilization,
        last_click_hour,
        last_click_timestamp,
        experiment_date,
        paced_day,
        non_paced_day
    from 
        pacing_days 
    where
        variant_day = "on" -- filters only to days where seller was paced or day before seller was paced
)
;

select
	non_paced_day,
	count(distinct shop_id) as shop_count
from
	`etsy-data-warehouse-dev.pdavidoff.paced_sellers_105`
group by 1
order by 1
;	
	

-- 19% of shops that were paced were only paced one day. 16% of shops that were paced were paced every day
with base as (
select
    shop_id,
    count(distinct experiment_date) as exp_days
from 
   `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105`
group by 1
)
select 
    exp_days/2 as exp_days,
    (count(distinct shop_id)/sum(count(distinct shop_id)) over()) as shop_count
from 
    base
group by 1
order by 1
;

-- look into percent of sellers that were paced on any given day. what percent of budget were they?
with base as (
    select 
        a.date,
        a.shop_id,
        a.budget,
        impressions_last_4w,
        case when b.shop_id is not null then 1 else 0 end as paced_seller,
        spend,
        seller_tier
    from
        `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a left join
        `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` b on a.shop_id = b.shop_id and a.date = b.paced_day left join 
        `etsy-data-warehouse-prod.rollups.seller_basics` c on a.shop_id = c.shop_id
    where
        date between "2021-10-04" and "2021-10-26"
)
select 
    paced_seller,
    count(distinct shop_id) as shop_count,
    count(distinct case when seller_tier in ("top seller","power seller") then shop_id end) as top_and_power,
    count(distinct case when impressions_last_4w > 0 then shop_id end) as adj_shops,
    -- count(distinct case when paced_seller = 1 then shop_id end)/count(distinct shop_id) as paced_shop_pct,
    -- count(distinct case when paced_seller = 1 and impressions_last_4w > 0 then shop_id end)/count(distinct case when impressions_last_4w > 0 then shop_id end) as paced_shop_adj_pct,
    sum(budget) as total_budget,
    sum(case when impressions_last_4w > 0 then budget end) as adj_budget,
    sum(spend) as total_spend
from 
    base
where
    mod(extract(day from date), 2) != 0
group by 1
order by 1
;

-- prolist ranking signals event
-- did pacing reduce the number of queries that don't have impressions?



-- what was the impact of pacing on sellers who were paced and sellers who weren"t
-- dataset is at the shop date level. a paced seller day is either:
-- 1. the day that shop was paced
-- 2. the day before that shop was paced
-- A: Sellers who were paced saw a significant increase in revenue, sellers who weren"t saw a decline. ROAS was pretty flat. 

create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.paced_browser_data_105` 
	as (
with browsers_temp as (
		select
	distinct
	visit_id,
	browser_id,
	datetime(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") as time,
	date(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") as date,
	(select value from unnest(properties.map) where key = "listing_id") as listing_id
FROM
  `etsy-visit-pipe-prod.canonical.visits` as a join
  UNNEST(a.events.events_tuple) as b on b.event_type = "prolist_imp_full"
where 
	a._date between "2021-10-04" and "2021-10-26"
	-- between "2021-10-04" and "2021-10-27"
)
select
	visit_id,
	browser_id,
	time,
	date,
	a.listing_id,
	b.shop_id
from
	browsers_temp a left join
	`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = cast(b.listing_id as string)
)
;




select
	date,
    case when mod(extract(day from date), 2) != 0 then "on" else "off" end as variant,
	count(distinct shop_id) as shop_count,
	count(distinct browser_id) as browser_count,
	count(distinct visit_id) as visit_count
from
	`etsy-data-warehouse-dev.pdavidoff.paced_browser_data_105`
group by 1,2
order by 1
;


-- data by pacing status and variant
with clicks as (
    select 
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") as date,
        shop_id,
        count(*) as clicks,
        sum(cost/100) as spend
    from `etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
    group by 1, 2, 3
),
ads_conv as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") as date,
        shop_id,
        count(*) as conv_clicks, 
        sum(revenue/100) as ads_gms
    from `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
        and date(timestamp(cast(timestamp_seconds(purchase_date) as datetime), "UTC"), "America/New_York")  
            <= date_add(date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York"), interval 1 day)
    group by 1,2,3
),
impressions as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") as date,
        a.shop_id,
        case when c.experiment_date is not null then 1 else 0 end as paced_seller,
        sum(a.impression_count) as impressions
    from `etsy-data-warehouse-prod.etsy_shard.shop_stats_prolist_snapshot_daily` a
    join `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` b
        on a.shop_id = b.shop_id 
            -- budget + util from two days prior!
            and date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") = date_add(b.date, interval 2 day)
    left join 
        `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` c on a.shop_id = c.shop_id and date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") = date(c.experiment_date)
    where date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
    group by 1,2,3,4
),browsers_distinct as (
    select distinct
        case when mod(extract(day from date), 2) != 0 then "on" else "off" end as variant,
        date,
        case when b.experiment_date is not null then 1 else 0 end as paced_seller,
        a.shop_id,
        visit_id,
        browser_id
    from `etsy-data-warehouse-dev.pdavidoff.paced_browser_data_105` a left join 
    `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` b on a.shop_id = b.shop_id and a.date = b.experiment_date
    where date >= date("2021-10-04") 
        and date <= date("2021-10-25")
),
browsers as (
    select
        a.variant,
        -- b.budget,
        -- b.budget_util,
        a.paced_seller,
        count(distinct visit_id) as visits,
        count(distinct browser_id) as browsers
    from browsers_distinct a
    join impressions b
        using (variant, date, shop_id)
    group by 1, 2
),
base as (
    select
        c.variant,
        case when d.experiment_date is not null then 1 else 0 end as paced_seller,
        -- c.budget,
        -- c.budget_util,
        min(c.date) as first_date,
        max(c.date) as max_date,
        sum(c.impressions) as impressions,
        sum(a.clicks) as clicks,
        sum(b.conv_clicks) as conv_clicks,
        sum(a.spend) as spend,
        sum(b.ads_gms) as ads_gms,
        sum(a.clicks) / sum(c.impressions) as ctr,
        sum(b.conv_clicks) / sum(a.clicks) as pccr,
        sum(b.ads_gms) / (sum(c.impressions) / 1000) as gpm,
        sum(b.ads_gms) / sum(a.spend) as roas
    from impressions c
    left join clicks a
        on c.variant = a.variant and c.shop_id = a.shop_id and c.date = a.date
    left join ads_conv b
        on c.variant = b.variant and c.shop_id = b.shop_id and c.date = b.date left join 
    `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` d on c.shop_id = d.shop_id and c.date = d.experiment_date
    group by 1, 2
)
select
    a.variant,
    a.paced_seller,
    -- first_date,
    -- max_date,
    -- a.budget,
    -- a.budget_util,
    b.browsers,
    b.visits,
    a.impressions,
    a.clicks,
    a.conv_clicks,
    a.spend,
    a.ads_gms,
    a.spend / b.browsers as spend_per_browser,
    a.impressions / b.browsers as impr_per_browser,
    a.spend / b.visits as spend_per_visit,
    a.impressions / b.visits as impr_per_visit,
    a.ctr,
    a.spend / a.clicks as cpc,
    a.pccr,
    a.gpm,
    a.roas
from base a
join browsers b
    on a.variant = b.variant and a.paced_seller = b.paced_seller
order by 2, 3, 1
;



-- the daily visit data is showing huge increases in visit/browser counts for paced sellers on on days
-- is this happening every day, or was there an outlier?
-- seems like it's happening pretty consistently every day
with impressions as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") as date,
        a.shop_id,
        case when c.experiment_date is not null then 1 else 0 end as paced_seller,
        sum(a.impression_count) as impressions
    from `etsy-data-warehouse-prod.etsy_shard.shop_stats_prolist_snapshot_daily` a
    join `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` b
        on a.shop_id = b.shop_id 
            -- budget + util from two days prior!
            and date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") = date_add(b.date, interval 2 day)
    left join 
        `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` c on a.shop_id = c.shop_id and date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") = date(c.experiment_date)
    where date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(a.reference_timestamp) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
    group by 1,2,3,4
),browsers_distinct as (
    select distinct
        case when mod(extract(day from date), 2) != 0 then "on" else "off" end as variant,
        date,
        case when b.experiment_date is not null then 1 else 0 end as paced_seller,
        a.shop_id,
        visit_id,
        browser_id
    from `etsy-data-warehouse-dev.pdavidoff.paced_browser_data_105` a left join 
    `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` b on a.shop_id = b.shop_id and a.date = b.experiment_date
    where date >= date("2021-10-04") 
        and date <= date("2021-10-25")
)
    select
        a.variant,
        -- b.budget,
        -- b.budget_util,
        a.date,
        count(distinct case when a.paced_seller = 1 then visit_id end) as visits_paced_seller,
        count(distinct case when a.paced_seller = 0 then visit_id end) as visits_no_pace_seller,
        count(distinct case when a.paced_seller = 1 then shop_id end) as shops_paced,
        count(distinct case when a.paced_seller = 0 then shop_id end) as shops_not_paced
        -- count(distinct browser_id) as browsers
    from browsers_distinct a
    join impressions b
        using (variant, date, shop_id)
    group by 1, 2
    order by 2
    ;

-- hourly data!
-- get the hourly data for the entire group, ignoring pacing status for now
with clicks as (
    select 
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        extract(hour from datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")) as hour,
        count(*) as clicks,
        sum(cost/100) as spend,
    from `etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
    group by 1, 2
),
ads_conv as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        extract(hour from datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")) as hour,
        count(*) as conv_clicks, 
        sum(revenue/100) as ads_gms
    from `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
        and datetime(timestamp(cast(timestamp_seconds(purchase_date) as datetime), "UTC"), "America/New_York")  
            <= date_add(datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York"), interval 1 day)
    group by 1, 2
),browsers_distinct as (
    select distinct
        case when mod(extract(day from date), 2) != 0 then "on" else "off" end as variant,
        date,
        extract(hour from time) as hour,
        -- case when b.experiment_date is not null then 1 else 0 end as paced_seller,
        a.shop_id,
        visit_id,
        browser_id
    from `etsy-data-warehouse-dev.pdavidoff.paced_browser_data_105` a
    -- `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` b on a.shop_id = b.shop_id and a.date = b.experiment_date
    where date >= date("2021-10-04") 
        and date <= date("2021-10-25")
),
browsers as (
    select
        variant,
        hour,
        -- b.budget,
        -- b.budget_util,
        count(distinct visit_id) as visits,
        count(distinct browser_id) as browsers,
        count(*) as impressions
    from browsers_distinct
    group by 1, 2
)
select
    a.variant,
    a.hour,
    d.browsers,
    d.visits,
    d.impressions,
    a.clicks,
    b.conv_clicks,
    a.spend,
    b.ads_gms,
    a.spend / d.browsers as spend_per_browser,
    a.spend / d.visits as spend_per_visit,
    d.impressions/visits as impressions_per_visit,
    a.clicks/visits as clicks_per_visits,
    a.clicks/impressions as ctr,
    a.spend / a.clicks as cpc,
    b.conv_clicks / a.clicks as pccr,
    b.ads_gms / a.spend as roas,
    d.impressions/browsers as impressions_per_browser,
    a.clicks/browsers as clicks_per_browser
from clicks a
full outer join ads_conv b
    using (variant, hour)
full outer join browsers d
    using (variant, hour)
order by 2, 1
;


-- hourly data to compare what's happening in the morning and afternoon
with clicks as (
    select 
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        extract(hour from datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")) as hour,
        -- extract(date from datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")) as date,
        case when b.shop_id is not null then 1 else 0 end as paced_seller,
        count(*) as clicks,
        sum(cost/100) as spend
    from 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` a left join 
        `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` b on a.shop_id = b.shop_id and extract(date from datetime(timestamp(cast(timestamp_seconds(a.click_date) as datetime), "UTC"), "America/New_York")) = b.experiment_date 
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
    group by 1, 2, 3
),
ads_conv as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        extract(hour from datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")) as hour,
        -- extract(date from datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")) as date,
        case when b.shop_id is not null then 1 else 0 end as paced_seller,
        count(*) as conv_clicks, 
        sum(revenue/100) as ads_gms
    from `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` a left join 
        `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` b on a.shop_id = b.shop_id and extract(date from datetime(timestamp(cast(timestamp_seconds(a.click_date) as datetime), "UTC"), "America/New_York")) = b.experiment_date
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
        and datetime(timestamp(cast(timestamp_seconds(purchase_date) as datetime), "UTC"), "America/New_York")  
            <= date_add(datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York"), interval 1 day)
    group by 1, 2, 3
),browsers_distinct as (
    select distinct
        case when mod(extract(day from date), 2) != 0 then "on" else "off" end as variant,
        date,
        extract(hour from time) as hour,
        case when b.experiment_date is not null then 1 else 0 end as paced_seller,
        a.shop_id,
        visit_id,
        browser_id
    from `etsy-data-warehouse-dev.pdavidoff.paced_browser_data_105` a left join 
    `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` b on a.shop_id = b.shop_id and a.date = b.experiment_date
    where date >= date("2021-10-04") 
        and date <= date("2021-10-25")
),
browsers as (
    select
        variant,
        hour,
        -- b.budget,
        -- b.budget_util,
        paced_seller,
        count(distinct visit_id) as visits,
        count(distinct browser_id) as browsers,
        count(*) as impressions
    from browsers_distinct
    group by 1, 2, 3
)
select
    -- a.variant,
    a.hour,
    a.paced_seller,
    sum(case when a.variant = "on" then browsers end)/sum(case when a.variant = "off" then browsers end) - 1 as browser_chg,   
    sum(case when a.variant = "on" then visits end)/sum(case when a.variant = "off" then visits end) - 1 as visits_chg,
    sum(case when a.variant = "on" then impressions end)/sum(case when a.variant = "off" then impressions end)-1 impr_chg,
    (sum(case when a.variant = "on" then clicks end)/sum(case when a.variant = "on" then impressions end))/(sum(case when a.variant = "off" then clicks end)/sum(case when a.variant = "off" then impressions end))-1 ctr,
    (sum(case when a.variant = "on" then spend end)/sum(case when a.variant = "on" then clicks end))/(sum(case when a.variant = "off" then spend end)/sum(case when a.variant = "off" then clicks end))-1 cpc,
    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,
    sum(spend) as total_spend,
    sum(impressions)/sum(sum(impressions)) over() as impression_share,
    sum(clicks)/sum(sum(clicks)) over() as click_share,
    sum(spend)/sum(sum(spend)) over() as spend_share
from clicks a
full outer join ads_conv b
    using (variant, hour, paced_seller)
full outer join browsers d
    using (variant, hour, paced_seller)
group by 1,2
order by 2,1
;


-- share of ad requests that didn't have ads
with base as (
select
	date(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") as date,
	datetime(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") as time,
	(select value from unnest(properties.map) where key = "query") as query,
	(select value from unnest(properties.map) where key = "listing_id") as listings
FROM
	`etsy-visit-pipe-prod.canonical.visits_sampled` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "prolist_ranking_signals"
WHERE
	date(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") 
	between date("2021-10-04") and date("2021-10-25") 
)
select
	case when mod(extract(day from date),2) != 0 then "on" else "off" end as variant,
	extract(hour from time) as hour, 
	count(*) as ad_requests,
	count(case when listings="[]" then query end) as empty_ad_requests,
	count(case when listings="[]" then query end)/count(*) as share_empty_requests
from 
	base
group by 1,2
order by 2,1
;

-- now that we've figured out that the new test has increased the number of visits with an impression,
-- we need to normalize our data to another metric. let's try to use prolist_ranking_signals
-- prolist ranking signals table
create or replace table 
    `etsy-data-warehouse-dev.pdavidoff.prolist_ranking_signals_105` 
    as (
        select
    distinct
    visit_id,
    browser_id,
    datetime(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") as time,
    date(timestamp(cast(timestamp_millis(b.epoch_ms) as datetime), "UTC"), "America/New_York") as date,
    (select value from unnest(properties.map) where key = "query") as query,
    (select value from unnest(properties.map) where key = "listing_id") as listings
FROM
  `etsy-visit-pipe-prod.canonical.visits` as a join
  UNNEST(a.events.events_tuple) as b on b.event_type = "prolist_ranking_signals"
where 
    (a._date between "2021-10-04" and "2021-10-26")
    -- (a._date between "2021-08-03" and "2021-08-17")
    -- between "2021-10-04" and "2021-10-27"
)
;



-- using prolist ranking signals does normalize the number of visits
-- however, spend is still trending down in the treatment by 1% driven by drop in CPC.
select
    case when mod(extract(day from date),2) != 0 then "on" else "off" end as variant,
    count(distinct browser_id) as ranking_sig_browsers,
    count(distinct visit_id) as ranking_sig_visits,
    count(case when listings != "[]" then visit_id end)/count(visit_id) as response_rate
from
    `etsy-data-warehouse-dev.pdavidoff.prolist_ranking_signals_105`
where
    date between "2021-10-04" and "2021-10-25"
group by 1
order by 1
;

-- get hourly prolist ranking signal events to get the number of visits
select
    case when mod(extract(day from date),2) != 0 then "on" else "off" end as variant,
    extract(hour from time) as hour,
    count(distinct browser_id) as rs_browser_count,
    count(distinct visit_id) as rs_visit_count
from
    `etsy-data-warehouse-dev.pdavidoff.prolist_ranking_signals_105`
where
    date between "2021-10-04" and "2021-10-25"
group by 1,2 
order by 2,1
;

select
    count(distinct query)
from
    `etsy-data-warehouse-dev.pdavidoff.prolist_ranking_signals_105`
;
-- which queries had the biggest improvement in impression rate?
with off_base as (
select
    query,
    count(*) as query_count,
    count(case when listings != "[]" then query end) as off_response,
    safe_divide(count(case when listings != "[]" then query end),count(query)) as off_query_share
from
    `etsy-data-warehouse-dev.pdavidoff.prolist_ranking_signals_105`
where
    mod(extract(day from date),2) = 0
group by 1
order by query_count desc
limit 100000
),on_base as (
select
    query,
    count(*) as query_count,
    count(case when listings != "[]" then query end) as on_response,
    safe_divide(count(case when listings != "[]" then query end),count(query)) as on_query_share
from
    `etsy-data-warehouse-dev.pdavidoff.prolist_ranking_signals_105`
where
    mod(extract(day from date),2) != 0
group by 1
order by query_count desc
limit 100000
),joins as (
select
    a.query,
    a.query_count as on_query_count,
    (a.off_query_share)*100 as off_query_share,
    b.query_count as off_query_count,
    (b.on_query_share)*100 as on_query_share,
    safe_divide(b.on_query_share,a.off_query_share) - 1 as pct_change,
    percent_rank() over(order by a.query_count+b.query_count) as pctile,
    on_response,
    off_response
from
    off_base a join
    on_base b on a.query = b.query
where
    safe_divide(a.off_query_share,b.on_query_share) - 1 is not null
order by 3
)
select
    case 
        when pctile <= .7 then "Tail"
        when pctile between .71 and .96 then "Torso"
        when pctile between .97 and 1 then "Head"
    end as query_bucket,
    count(distinct query) as query_count,
    query,
    sum(on_response)/sum(on_query_count) as on_response_rate,
    sum(off_response)/sum(off_query_count) as off_response_rate,
    safe_divide((sum(on_response)/sum(on_query_count)),(sum(off_response)/sum(off_query_count))) - 1 as on_response_chg
from
    joins
group by 1
order by 3
;

-- average exhaust times

create or replace table 
    `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime` 
    as (
with clicks as (
    select 
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(a.click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        extract(hour from datetime(timestamp(cast(timestamp_seconds(a.click_date) as datetime), "UTC"), "America/New_York")) as hour,
        date(timestamp(cast(timestamp_seconds(a.click_date) as datetime), "UTC"), "America/New_York") as date,
        a.shop_id,
        b.budget,
        c.seller_tier,
        count(*) as clicks,
        sum(round(a.cost/100, 2)) as spend
    from `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` a
    join `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` b
        on a.shop_id = b.shop_id 
            and date(timestamp(cast(timestamp_seconds(a.click_date) as datetime), "UTC"), "America/New_York") = b.date
    join `etsy-data-warehouse-prod.rollups.seller_basics` c
        on a.shop_id = c.shop_id
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-25")
    group by 1, 2, 3, 4, 5, 6
),
ads_conv as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")), 2) != 0 then "on" else "off" end as variant,
        extract(hour from datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York")) as hour,
        date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") as date,
        shop_id,
        count(*) as conv_clicks, 
        sum(revenue/100) as ads_gms
    from `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-26")
        and datetime(timestamp(cast(timestamp_seconds(purchase_date) as datetime), "UTC"), "America/New_York")  
            <= date_add(datetime(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York"), interval 1 day)
    group by 1, 2, 3, 4
),
base as (
    select
        a.variant,
        a.date,
        case when c.shop_id is not null then 1 else 0 end as paced_seller,
        a.shop_id,
        a.seller_tier,
        a.hour,
        a.budget,
        a.clicks,
        b.conv_clicks,
        a.spend,
        b.ads_gms,
        sum(a.spend) over (partition by a.shop_id, date order by hour asc) as cumulative_spend
    from clicks a
    left join ads_conv b
        using (variant, hour, date, shop_id)
    left join `etsy-data-warehouse-dev.pdavidoff.paced_sellers_105` c on a.shop_id = c.shop_id and a.date = c.experiment_date
)
    select 
        variant,
        date,
        shop_id,
        paced_seller,
        seller_tier,
        budget,
        sum(clicks) as clicks,
        sum(conv_clicks) as conv_clicks,
        sum(spend) as spend,
        sum(ads_gms) as ads_gms,
        min(case when round(cumulative_spend/ budget, 1) >= 1.0 then hour end) as time_to_exhaust
    from base
    group by 1, 2, 3, 4, 5, 6
)
;


-- distribution of exhaust times overall
select
    time_to_exhaust,
    count(case when variant = "on" then 1 end) as on_campaigns,
    count(case when variant = "off" then 1 end) as off_campaigns
from
    `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime`
where seller_tier in ("top seller", "power seller")
group by 1
order by 1
;

-- distribution of exhaust times paced sellers

select 
    time_to_exhaust,
    count(case when variant = "on" then 1 end) as on_campaigns,
    count(case when variant = "off" then 1 end) as off_campaigns
from 
    `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime`
where
    paced_seller = 1
group by 1
order by 1
;

select 
    time_to_exhaust,
    count(case when variant = 'on' then 1 end) as on_campaigns,
    count(case when variant = "off" then 1 end) as off_campaigns
from 
    `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime`
where
    paced_seller = 0
group by 1
order by 1
;
-- average
select distinct
    variant,
    avg(time_to_exhaust) as avg_overall,
    avg(case when paced_seller = 1 then time_to_exhaust end) as avg_paced
from 
    `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime`
group by 1
order by 1
;

-- median (power + top)
select distinct
    variant,
    percentile_cont(time_to_exhaust, 0.5) OVER (partition by variant) AS median_overall
from `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime`
where seller_tier in ("top seller", "power seller")
order by 1
;

-- median (paced seller)
select distinct
    variant,
    percentile_cont(time_to_exhaust, 0.5) OVER (partition by variant) AS median_overall
from `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime`
where paced_seller = 1
order by 1
;

-- median for overall group
select distinct
    variant,
    percentile_cont(time_to_exhaust, 0.5) OVER (partition by variant) AS median_overall
from `etsy-data-warehouse-dev.pdavidoff.paced_campaign_lifetime`
-- where paced_seller = 0
order by 1
;

-- what was daily cpc, pccr, and roas
-- DAILY

with click_tab as (
    select 
        date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") as date,
        count(*) as clicks,
        sum(cost/100) as spend
    from 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
    where 
        date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-26") 
    group by 1
),purchase_tab as (
    select
        date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") as date,
        count(*) as conv_clicks, 
        sum(revenue/100) as ads_gms
    from `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York") <= date("2021-10-26")
        and date(timestamp(cast(timestamp_seconds(purchase_date) as datetime), "UTC"), "America/New_York")  
            <= date_add(date(timestamp(cast(timestamp_seconds(click_date) as datetime), "UTC"), "America/New_York"), interval 1 day)
    group by 1
)
select
    case when mod(extract(day from a.date), 2) != 0 then "on" else "off" end as variant,
    a.date,
    clicks,
    spend,
    conv_clicks,
    ads_gms,
    spend/clicks as cpc,
    conv_clicks/clicks as pccr,
    ads_gms/spend as roas
from 
    click_tab a full outer join
    purchase_tab b using (date)
order by 2
;




