with clicks as (
    select 
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), 'UTC'), 'America/New_York')), 2) != 0 then "on" else "off" end as variant,
        count(*) as clicks,
        sum(cost/100) as spend,
    from `etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), 'UTC'), 'America/New_York') >= date("2021-10-04")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), 'UTC'), 'America/New_York') <= date("2021-10-26")
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), 'UTC'), 'America/New_York') <=
        date_sub(current_date(),interval 2 day)
    group by 1
),
ads_conv as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(click_date) as datetime), 'UTC'), 'America/New_York')), 2) != 0 then "on" else "off" end as variant,
        count(*) as conv_clicks, 
        sum(revenue/100) as ads_gms
    from `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days`
    where date(timestamp(cast(timestamp_seconds(click_date) as datetime), 'UTC'), 'America/New_York') >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(click_date) as datetime), 'UTC'), 'America/New_York') <= date("2021-10-26")
        and date(timestamp(cast(timestamp_seconds(purchase_date) as datetime), 'UTC'), 'America/New_York')  
            <= date_sub(current_date(),interval 2 day)
    group by 1
),
impressions as (
    select
        case when mod(extract(day from date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), 'UTC'), 'America/New_York')), 2) != 0 then "on" else "off" end as variant,
        sum(impression_count) as impressions
    from `etsy-data-warehouse-prod.etsy_shard.shop_stats_prolist_snapshot_daily`
    where date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), 'UTC'), 'America/New_York') >= date("2021-10-04") 
        and date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), 'UTC'), 'America/New_York') <= date("2021-10-26")
        and date(timestamp(cast(timestamp_seconds(reference_timestamp) as datetime), 'UTC'), 'America/New_York') <= date_sub(current_date(),interval 2 day)
    group by 1
),
browsers_temp as (
    select distinct
        visit_id,
        split(visit_id, ".")[offset(0)] as browser_id,
        date(timestamp(cast(timestamp_millis(epoch_ms) as datetime), 'UTC'), 'America/New_York') as date
    from `etsy-data-warehouse-prod.weblog.events`
    where _date >= date("2021-10-04")
        and event_type = "prolist_imp_full"
        and date(timestamp_seconds(run_date)) >= date("2021-10-04")
        and date(timestamp_seconds(run_date)) <= date("2021-10-27")
        and date(timestamp(cast(timestamp_millis(epoch_ms) as datetime), 'UTC'), 'America/New_York') <= date_sub(current_date(),interval 2 day)
),
browsers as (
    select  
        case when mod(extract(day from date), 2) != 0 then "on" else "off" end as variant,
        count(*) as visits,
        count(distinct browser_id) as browsers
    from browsers_temp
    where date >= date("2021-10-04") 
        and date <= date("2021-10-26")
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