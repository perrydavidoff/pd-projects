-- summary stats for each of the new ad treatments
-- zooming, hover to play, triplet, and multi-image were analyzed
-- this table has every impression from the five experiment types. some of the older large ad treatment experiments are below
create or replace table 
    `etsy-data-warehouse-dev.pdavidoff.large_ads_2022` 
    as (
    select 
        _PARTITIONDATE as date,
        visit_id,
        uuid,
        -- click,
        add_cart,
        fave,
        a.query,
        position,
        abvariant as ab_variant,
        timestamp_seconds(cast(timestamp as int64)) as impression_time,
        a.page_type,
        logging_key,
        a.shop_id,
        a.listing_id,
        -- cost,
        predctr,
        activeAdsAbVariants,
        specialadtreatments,
        full_purchase_label,
        -- ab variants for desktop search
         case 
            when activeAdsAbVariants like "%ranking/badx.2022_q1.large_ads_search.desktop:off%" then "off"
            when activeAdsAbVariants like "%ranking/badx.2022_q1.large_ads_search.desktop:large_ad%" then "large_ad"
            when activeAdsAbVariants like "%ranking/badx.2022_q1.large_ads_search.desktop:more_ads%" then "more_ads"
            else null
        end as desktop_search_ab_variant,
        -- ab variants for mweb search
        case 
            when activeAdsAbVariants like "%ranking/badx.2022_q1.large_ads_search.mweb:off%" then "off"
            when activeAdsAbVariants like "%ranking/badx.2022_q1.large_ads_search.mweb:large_ad%" then "large_ad"
            when activeAdsAbVariants like "%ranking/badx.2022_q1.large_ads_search.mweb:more_ads%" then "more_ads"
            else null
        end as mweb_search_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        null as desktop_market_ab_variant_detail,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        "large ads" as nat_test,
        case when position = 6 then 1 else 0 end as analysis_flag
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE >= "2022-02-09" and 
        activeAdsAbVariants like "%2022_q1.large_ads_search%"
)
    ;



-- desktop performance to start
-- the experiment started on feb 9
select
    desktop_search_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_2022`
where
    desktop_search_ab_variant is not null
group by 1
order by 1 desc
;

-- now mobile web
 select
    mweb_search_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_2022`
where
    mweb_search_ab_variant is not null and
    date >= "2022-02-12"
group by 1
order by 1 desc
;

-- position 6 for ads is the one with large ad on desktop
select
    desktop_search_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_2022`
where
    desktop_search_ab_variant is not null and analysis_flag = 1
group by 1
order by 1 desc
;

-- position 6 for ads is the one with large ad on mweb
select
    mweb_search_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_2022`
where
    mweb_search_ab_variant is not null and analysis_flag = 1
group by 1
order by 1 desc
;


-- do the same for the market pages
create or replace table 
    `etsy-data-warehouse-dev.pdavidoff.large_ads_market_2021` 
    as (
with base as (
    select 
        _PARTITIONDATE as date,
        visit_id,
        (split(visit_id, ".")[ORDINAL(1)]) as browser_id,
        uuid,
        -- click,
        add_cart,
        fave,
        a.query,
        position,
        abvariant as ab_variant,
        timestamp_seconds(cast(timestamp as int64)) as impression_time,
        a.page_type,
        logging_key,
        a.shop_id,
        a.listing_id,
        -- cost,
        predctr,
        activeAdsAbVariants,
        specialadtreatments,
        full_purchase_label,
        -- ab variants for desktop search
         case 
            when activeAdsAbVariants like "%ranking/badx.2021_q4.market_page_large_ad.desktop:off%" then "off"
            when activeAdsAbVariants like "%ranking/badx.2021_q4.market_page_large_ad.desktop:large_ad%" then "large_ad"
            when activeAdsAbVariants like "%ranking/badx.2021_q4.market_page_large_ad.desktop:more_ads%" then "more_ads"
            else null
        end as desktop_market_ab_variant,
        -- ab variants for mweb search
        case 
            when activeAdsAbVariants like "%ranking/badx.2021_q4.market_page_large_ad.mweb:off%" then "off"
            when activeAdsAbVariants like "%ranking/badx.2021_q4.market_page_large_ad.mweb:large_ad%" then "large_ad"
            when activeAdsAbVariants like "%ranking/badx.2021_q4.market_page_large_ad.mweb:more_ads%" then "more_ads"
            else null
        end as mweb_market_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        null as desktop_market_ab_variant_detail,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        "large ads" as nat_test
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE >= "2021-12-09" and 
        activeAdsAbVariants like "%ranking/badx.2021_q4.market_page_large_ad%"
)
select
    *,
    case 
        when ((position = 4 and mweb_market_ab_variant is not null) or (position = 8 and desktop_market_ab_variant is not null)) then 1 
        else 0 
    end as analysis_flag
from
    base
)
    ;

select
    position,
    count(*)
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_market_2021` 
where
    specialadtreatments like "%large%"
    and desktop_market_ab_variant = "large_ad"
group by 1
order by 1
;

-- position 6 for ads is the one with large ad on desktop
-- overall experiment results
select
    desktop_market_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_market_2021`
where
    desktop_market_ab_variant is not null
group by 1
order by 1 desc
;

select
    desktop_market_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_market_2021`
where
    desktop_market_ab_variant is not null and analysis_flag = 1
group by 1
order by 1 desc
;

select
    desktop_market_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_market_2021`
where
    desktop_market_ab_variant is not null and analysis_flag = 1
group by 1
order by 1 desc
;

-- position 6 for ads is the one with large ad on mweb
select
    mweb_market_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_market_2021`
where
    mweb_market_ab_variant is not null
group by 1
order by 1 desc
;

-- just position of large ad (position 4)
select
    mweb_market_ab_variant,
    count(distinct visit_id) as visit_count,
    count(visit_id)/count(distinct visit_id) as imps_per_visit,
    count(case when click = 1 then visit_id end)/count(visit_id) as ctr,
    sum(case when click = 1 then cost/100 end)/count(case when click = 1 then visit_id end) as cpc,
    sum(revenue/100)/count(distinct visit_id) as revenue_per_visit,
    count(case when purchase = 1 then visit_id end)/count(case when click = 1 then visit_id end) as pccr,
    sum(revenue/100)/count(case when purchase = 1 then visit_id end) as gms_per_purchase,
    sum(revenue/100)/sum(cost/100) as roas
from
    `etsy-data-warehouse-dev.pdavidoff.large_ads_market_2021`
where
    mweb_market_ab_variant is not null and analysis_flag = 1
group by 1
order by 1 desc
;