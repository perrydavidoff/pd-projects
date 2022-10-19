-- this table has every impression from the five experiment types. some of the older large ad treatment experiments are below
create or replace table etsy-data-warehouse-dev.pdavidoff.new_ad_treatments as (
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
            when activeAdsAbVariants like '%zooming_ads.search.desktop:off%' then 0
            when activeAdsAbVariants like '%zooming_ads.search.desktop:on%' then 1
            else null
        end as desktop_search_ab_variant,
        -- ab variants for market search
         case 
            when activeAdsAbVariants like '%zooming_ads.market.desktop:off%' then 0
            when activeAdsAbVariants like '%zooming_ads.market.desktop:on%' then 1
            else null
        end as desktop_market_ab_variant,
        -- ab variants for market search
         case 
            when activeAdsAbVariants like '%zooming_ads.market.mweb:off%' then 0
            when activeAdsAbVariants like '%zooming_ads.market.mweb:on%' then 1
            else null
        end as mweb_market_ab_variant,
        -- ab variants for mweb search
        case 
            when activeAdsAbVariants like '%zooming_ads.search.mweb:off%' then 0
            when activeAdsAbVariants like '%zooming_ads.search.mweb:on%' then 1
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
        1 as zooming_ads_experiment,
        0 as multi_listing_ads_experiment,
        0 as multi_image_ads_experiment,
        0 as large_ads_experiment,
        0 as hover_to_play_experiment,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        'zooming' as nat_test,
        case when position = 1 then 1 else 0 end as analysis_flag
        -- case when activeAdsAbVariants like '%zooming_ads%' then 1 else 0 end as zooming_ads_experiment,
        -- case when activeAdsAbVariants like '%multi_listing_ads_market_page%' then 1 else 0 end as multi_listing_ads_experiment,
        -- case when activeAdsAbVariants like '%badx.2021_q2.multiimage_ads_market_page%' then 1 else 0 end as multi_image_ads_experiment,
        -- case when activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page%' then 1 else 0 end as large_ads_experiment,
        -- case when activeAdsAbVariants like '%ranking/badx.2021_q3.ads_only_listing_card_video_on_search%' then 1 else 0 end as hover_to_play_experiment
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE between '2021-09-01' and '2021-09-09' and 
        activeAdsAbVariants like '%zooming_ads%'
UNION ALL
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
         null as search_desktop_ab_variant,
        -- ab variants for market search
         case 
            when activeAdsAbVariants like '%badx.2021_q3.multi_listing_ads_market_page.desktop:off%' then 0
            when activeAdsAbVariants like '%badx.2021_q3.multi_listing_ads_market_page.desktop:on%' then 1
            else null
        end as desktop_market_ab_variant,
        -- ab variants for mweb market
         case 
            when activeAdsAbVariants like '%badx.2021_q3.multi_listing_ads_market_page.mweb:off%' then 0
            when activeAdsAbVariants like '%badx.2021_q3.multi_listing_ads_market_page.mweb:on%' then 1
            else null
        end as mweb_market_ab_variant,
        -- ab variants for mweb search
        null as mweb_search_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        null as desktop_market_ab_variant_detail,
        0 as zooming_ads_experiment,
        1 as multi_listing_ads_experiment,
        0 as multi_image_ads_experiment,
        0 as large_ads_experiment,
        0 as hover_to_play_experiment,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        'triplet' as nat_test,
        case 
            when (activeAdsAbVariants like '%multi_listing_ads_market_page.mweb%' and position = 2)
            or (activeadsAbVariants like '%multi_listing_ads_market_page.desktop%' and position = 8)
         then 1 else 0 end as analysis_flag
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE between '2021-07-14' and '2021-07-20' and 
        activeAdsAbVariants like '%multi_listing_ads_market_page%'
-- the multi image test introduced multiple images for the same listing
UNION ALL
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
         null as search_desktop_ab_variant,
        -- ab variants for market search
         null as desktop_market_ab_variant,
        -- ab variants for mweb market
         case 
            when activeAdsAbVariants like '%badx.2021_q2.multiimage_ads_market_page:off%' then 0
            when activeAdsAbVariants like '%badx.2021_q2.multiimage_ads_market_page:on%' then 1
            else null
        end as mweb_market_ab_variant,
        -- ab variants for mweb search
        null as mweb_search_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        null as desktop_market_ab_variant_detail,
        0 as zooming_ads_experiment,
        0 as multi_listing_ads_experiment,
        1 as multi_image_ads_experiment,
        0 as large_ads_experiment,
        0 as hover_to_play_experiment,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        'multi-image' as nat_test,
        case when position in (4) then 1 else 0 end as analysis_flag

    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE between '2021-04-27' and '2021-05-03' and 
        activeAdsAbVariants like '%badx.2021_q2.multiimage_ads_market_page%'
UNION ALL 
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
         null as search_desktop_ab_variant,
        -- ab variants for desktop market
         case 
            when activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page:off%' then 0
            when activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page%' and 
            (activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page:large_ad_right%' or activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page:large_ad_left%') then 1
            else null
        end as desktop_market_ab_variant,
        -- ab variants for mweb market
        null as mweb_market_ab_variant,
        -- ab variants for mweb search
        null as mweb_search_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        case 
            when activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page:large_ad_right%' then 'right'
            when activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page:large_ad_left%' then  'left'
        end as desktop_market_ab_variant_detail,
        0 as zooming_ads_experiment,
        0 as multi_listing_ads_experiment,
        0 as multi_image_ads_experiment,
        1 as large_ads_experiment,
        0 as hover_to_play_experiment,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        'large ad' as nat_test,
        case when position in (4,6,8) then 1 else 0 end as analysis_flag
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE between '2021-04-27' and '2021-05-03' and 
        activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page%'
UNION ALL
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
            when activeAdsAbVariants like '%ranking/badx.2021_q3.ads_only_listing_card_video_on_search:off%' then 0
            when activeAdsAbVariants like '%ranking/badx.2021_q3.ads_only_listing_card_video_on_search:on%' then 1
            else null
        end as desktop_search_ab_variant,
        -- ab variants for market search
         null as desktop_market_ab_variant,
        -- ab variants for mweb market
        null as mweb_market_ab_variant,
        -- ab variants for mweb search
        null as mweb_search_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        null as desktop_market_ab_variant_detail,
        0 as zooming_ads_experiment,
        0 as multi_listing_ads_experiment,
        0 as multi_image_ads_experiment,
        0 as large_ads_experiment,
        1 as hover_to_play_experiment,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        'video' as nat_test,
        case when position in (0,1,2,3) then 1 else 0 end as analysis_flag
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE between '2021-08-21' and '2021-08-29' and 
        activeAdsAbVariants like '%ranking/badx.2021_q3.ads_only_listing_card_video_on_search%'
)
;

select
    position,
    count(*)
from 
    `etsy-prolist-etl-prod.prolist.attributed_impressions_market_large_ads_mweb`
where
    is_large_format = 1
group by 1
order by 1
;

-- 2020 large ads treatments are in this table. we ran this on mweb and desktop.
create or replace table etsy-data-warehouse-dev.pdavidoff.nat_large_ads as (
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
        'large_ads_mweb_market_2020' as ab_experiment,
        case when is_large_format = 1 then 'large_ad' else '' end as specialadtreatments,
        -- ab variants for desktop search
        0 as desktop_search_ab_variant,
        -- ab variants for market search
        0 as desktop_market_ab_variant,
        -- ab variants for market search
        1 as mweb_market_ab_variant,
        -- ab variants for mweb search
        0 as mweb_search_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        null as desktop_market_ab_variant_detail,
        1 as mweb_large_ads_2020_experiment,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        'large ads 2020' as nat_test,
        case when position in (2,5) then 1 else 0 end as analysis_flag
        -- case when activeAdsAbVariants like '%zooming_ads%' then 1 else 0 end as zooming_ads_experiment,
        -- case when activeAdsAbVariants like '%multi_listing_ads_market_page%' then 1 else 0 end as multi_listing_ads_experiment,
        -- case when activeAdsAbVariants like '%badx.2021_q2.multiimage_ads_market_page%' then 1 else 0 end as multi_image_ads_experiment,
        -- case when activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page%' then 1 else 0 end as large_ads_experiment,
        -- case when activeAdsAbVariants like '%ranking/badx.2021_q3.ads_only_listing_card_video_on_search%' then 1 else 0 end as hover_to_play_experiment
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions_market_large_ads_mweb` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE between '2020-11-10' and '2020-11-17' and abvariant != 'other'
UNION ALL
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
        'large_ads_mweb_market_2020' as ab_experiment,
        case when is_large_format = 1 then 'large_ad' else '' end as specialadtreatments,
        -- ab variants for desktop search
        0 as desktop_search_ab_variant,
        -- ab variants for market search
        0 as desktop_market_ab_variant,
        -- ab variants for market search
        1 as mweb_market_ab_variant,
        -- ab variants for mweb search
        0 as mweb_search_ab_variant,
        case when b.plkey is not null then 1 else 0 end as purchase,
        b.revenue,
        b.revenue_subtotal,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then 1 else 0 end as purchase_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then revenue end as revenue_1d,
        case when date_diff(timestamp_seconds(b.purchase_date),timestamp_seconds(b.click_date),day) <= 1 then receipt_id end as receipt_id_1d,
        b.receipt_id,
        b.purchase_date,
        null as desktop_market_ab_variant_detail,
        1 as mweb_large_ads_2020_experiment,
        case when c.plkey is not null then 1 else 0 end as click,
        c.cost,
        'large ads 2020' as nat_test,
        case when position in (2,5) then 1 else 0 end as analysis_flag
        -- case when activeAdsAbVariants like '%zooming_ads%' then 1 else 0 end as zooming_ads_experiment,
        -- case when activeAdsAbVariants like '%multi_listing_ads_market_page%' then 1 else 0 end as multi_listing_ads_experiment,
        -- case when activeAdsAbVariants like '%badx.2021_q2.multiimage_ads_market_page%' then 1 else 0 end as multi_image_ads_experiment,
        -- case when activeAdsAbVariants like '%badx.2021_q2.large_ads_market_page%' then 1 else 0 end as large_ads_experiment,
        -- case when activeAdsAbVariants like '%ranking/badx.2021_q3.ads_only_listing_card_video_on_search%' then 1 else 0 end as hover_to_play_experiment
    from 
        `etsy-prolist-etl-prod.prolist.attributed_impressions_market_large_ads_desktop` a left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_attribution_30_days` b on a.logging_key = b.plkey left join 
        `etsy-data-warehouse-prod.etsy_shard.prolist_click_log` c on a.logging_key = c.plkey
    where
        _PARTITIONDATE between '2020-11-10' and '2020-11-17' and abvariant != 'other'

