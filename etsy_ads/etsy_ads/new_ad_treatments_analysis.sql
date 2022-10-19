-- summary stats for each of the new ad treatments
-- zooming, hover to play, triplet, and multi-image were analyzed
select count(*) from `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`;

select
    count(*)
from
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
;

select count(*) from `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`;
with trans_data as (
    select
        a.logging_key,
        a.receipt_id,
        count(distinct transaction_id) as transaction_count,
        sum(usd_subtotal_price)/sum(quantity) as aiv,
        sum(quantity) as total_quantity
    from
        `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments` a left join 
        `etsy-data-warehouse-prod.transaction_mart.all_transactions` b on a.receipt_id_1d = b.receipt_id
    group by 1,2
),treatment_base as (
select
    nat_test, 
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    transaction_count as on_trans_count,
    aiv as on_aiv,
    total_quantity as on_quantity,
    -- listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as on_visit_count,
    count(*) as on_impressions,
    sum(click) as on_clicks,
    sum(purchase_1d) as on_purchases,
    sum(case when click = 1 then cost end) as on_cost,
    sum(revenue_1d) as on_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments` a left join 
    trans_data b on a.logging_key = b.logging_key
where 
    -- experiment
    analysis_flag = 1 and
    -- pages
    ((desktop_search_ab_variant = 1 and page_type = 0) or (desktop_market_ab_variant = 1 and page_type = 1)
    or (mweb_search_ab_variant = 1 and page_type = 0) or (mweb_market_ab_variant = 1 and page_type = 1)) 
    -- ad treatment type (if treatment)
    and ((specialadtreatments = 'zooming' and zooming_ads_experiment = 1) or (specialadtreatments like '%video%' and hover_to_play_experiment = 1) 
    or (specialadtreatments = 'multi-listing-ad' and multi_listing_ads_experiment = 1) or (specialadtreatments = 'multi-image-ad' and multi_image_ads_experiment = 1) or
    (specialadtreatments like '%offset%' and large_ads_experiment = 1))
group by 1,2,3,4,5,6
),control_base as (
select 
    nat_test,
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    transaction_count as off_trans_count,
    aiv as off_aiv,
    total_quantity as off_quantity,
    -- listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as off_visit_count,
    count(*) as off_impressions,
    sum(click) as off_clicks,
    sum(purchase_1d) as off_purchases,
    sum(case when click = 1 then cost end) as off_cost,
    sum(revenue_1d) as off_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments` a left join 
    trans_data b on a.logging_key = b.logging_key
where 
    -- experiment
    analysis_flag = 1 and
    -- pages
    ((desktop_search_ab_variant = 0 and page_type = 0) or (desktop_market_ab_variant = 0 and page_type = 1)
    or (mweb_search_ab_variant = 0 and page_type = 0) or (mweb_market_ab_variant = 0 and page_type = 1)) 
group by 1,2,3,4,5,6
)
select
    a.nat_test,
    a.platform,
    case when a.page_type = 0 then 'search' else 'market' end as page,
    sum(a.on_visit_count) as on_visit_count,
    sum(b.off_visit_count) as off_visit_count,
    -- count(distinct a.listing_id) as on_listing_count,
    -- count(distinct b.listing_id) as off_listing_count,
    (sum(on_impressions)/sum(on_visit_count))/(sum(off_impressions)/sum(off_visit_count)) - 1 as impr_chg,
    (sum(on_cost)/sum(on_clicks))/(sum(off_cost)/sum(off_clicks)) - 1 as cpc_chg,
    (sum(on_clicks)/sum(on_impressions))/(sum(off_clicks)/sum(off_impressions)) - 1 as ctr_chg,
    (sum(on_purchases)/sum(on_clicks))/(sum(off_purchases)/sum(off_clicks)) - 1 as pccvr_chg,
    (sum(on_purchase_amt)/sum(on_cost))/(sum(off_purchase_amt)/sum(off_cost)) - 1 as roas_chg,
    (sum(on_purchase_amt)/sum(on_purchases))/(sum(off_purchase_amt)/sum(off_purchases))-1 as ea_aov_chg,
    sum(on_impressions) as on_impressions,
    sum(on_purchases) as on_purchases,
    sum(off_impressions) as off_impressions,
    sum(off_purchases) as off_purchases,
    (sum(on_quantity)/sum(on_purchases))/(sum(off_quantity)/sum(off_purchases))-1 as quantity_per_order_change,
    (sum(on_purchase_amt)/sum(on_quantity))/(sum(off_purchase_amt)/sum(off_quantity))-1 as aiv_change,   
    (sum(on_trans_count)/sum(on_purchases))/(sum(off_trans_count)/sum(off_purchases))-1 as trans_per_order_change
from 
    treatment_base a join
    control_base b on a.page_type = b.page_type and a.platform = b.platform and a.nat_test = b.nat_test
group by 1,2,3
order by 1,2 desc
;


-- video treated separately because only certain listings have video enabled
-- join on listing_id to make sure that the listings are consistent
with treatment_base as (
select 
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    -- position,
    -- position,
    listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as on_visit_count,
    count(*) as on_impressions,
    sum(click) as on_clicks,
    sum(purchase) as on_purchases,
    sum(cost) as on_cost,
    sum(revenue) as on_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where 
    -- experiment
    hover_to_play_experiment = 1 and 
    -- pages
    ((desktop_search_ab_variant = 1 and page_type = 0) or (desktop_market_ab_variant = 1 and page_type = 1)
    or (mweb_search_ab_variant = 1 and page_type = 0) or (mweb_market_ab_variant = 1 and page_type = 1)) 
    -- ad treatment type (if treatment)
    and specialadtreatments like '%video%'
    -- position (if the treatment was only in a specific position)
    and position in (0,1,2,3)
group by 1,2,3
),control_base as (
select 
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    -- position,
    -- position,
    listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as off_visit_count,
    count(*) as off_impressions,
    sum(click) as off_clicks,
    sum(purchase) as off_purchases,
    sum(cost) as off_cost,
    sum(revenue) as off_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where 
    -- experiment
    hover_to_play_experiment = 1 and 
    -- pages
    ((desktop_search_ab_variant = 0 and page_type = 0) or (desktop_market_ab_variant = 0 and page_type = 1)
    or (mweb_search_ab_variant = 0 and page_type = 0) or (mweb_market_ab_variant = 0 and page_type = 1)) 
    -- ad treatment type (if treatment)
    -- position (if the treatment was only in a specific position)
    and position in (0,1,2,3)
group by 1,2,3
)
select
    a.platform,
    case when a.page_type = 0 then 'search' else 'market' end as page,
    -- a.position,
    sum(a.on_visit_count) as on_visit_count,
    sum(b.off_visit_count) as off_visit_count,
    -- count(distinct a.listing_id) as on_listing_count,
    -- count(distinct b.listing_id) as off_listing_count,
    (sum(on_impressions)/sum(on_visit_count))/(sum(off_impressions)/sum(off_visit_count)) - 1 as impr_chg,
    (sum(on_cost)/sum(on_clicks))/(sum(off_cost)/sum(off_clicks)) - 1 as cpc_chg,
    (sum(on_clicks)/sum(on_impressions))/(sum(off_clicks)/sum(off_impressions)) - 1 as ctr_chg,
    (sum(on_purchases)/sum(on_clicks))/(sum(off_purchases)/sum(off_clicks)) - 1 as pccvr_chg,
    (sum(on_purchase_amt)/sum(on_cost))/(sum(off_purchase_amt)/sum(off_cost)) - 1 as roas_chg,
    sum(on_impressions) as on_impressions,
    sum(on_purchases) as on_purchases,
    sum(off_impressions) as off_impressions,
    sum(off_purchases) as off_purchases
from 
    treatment_base a join
    control_base b on a.page_type = b.page_type and a.platform = b.platform and a.listing_id = b.listing_id
group by 1,2
order by 1,2,3
;

-- large ads left offset
select
    position,
    count(*)
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where
    large_ads_experiment = 1 and specialadtreatments is not null and desktop_market_ab_variant_detail = 'right'
group by 1
order by 1
;
-- left offset
with treatment_base as (
select 
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    -- position,
    -- position,
    listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as on_visit_count,
    count(*) as on_impressions,
    sum(click) as on_clicks,
    sum(purchase) as on_purchases,
    sum(cost) as on_cost,
    sum(revenue) as on_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where 
    -- experiment
    large_ads_experiment = 1 and 
    -- pages
    ((desktop_search_ab_variant = 1 and page_type = 0) or (desktop_market_ab_variant = 1 and page_type = 1)
    or (mweb_search_ab_variant = 1 and page_type = 0) or (mweb_market_ab_variant = 1 and page_type = 1)) 
    and desktop_market_ab_variant_detail = 'left'
    -- ad treatment type (if treatment)
    and specialadtreatments like '%offset%'
    -- position (if the treatment was only in a specific position)
    and position = 10
group by 1,2,3
),control_base as (
select 
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    -- position,
    -- position,
    listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as off_visit_count,
    count(*) as off_impressions,
    sum(click) as off_clicks,
    sum(purchase) as off_purchases,
    sum(cost) as off_cost,
    sum(revenue) as off_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where 
    -- experiment
    large_ads_experiment = 1 and 
    -- pages
    ((desktop_search_ab_variant = 0 and page_type = 0) or (desktop_market_ab_variant = 0 and page_type = 1)
    or (mweb_search_ab_variant = 0 and page_type = 0) or (mweb_market_ab_variant = 0 and page_type = 1)) 
    -- ad treatment type (if treatment)
    -- position (if the treatment was only in a specific position)
    and position = 10
group by 1,2,3
)
select
    a.platform,
    case when a.page_type = 0 then 'search' else 'market' end as page,
    -- a.position,
    sum(a.on_visit_count) as on_visit_count,
    sum(b.off_visit_count) as off_visit_count,
    -- count(distinct a.listing_id) as on_listing_count,
    -- count(distinct b.listing_id) as off_listing_count,
    (sum(on_impressions)/sum(on_visit_count))/(sum(off_impressions)/sum(off_visit_count)) - 1 as impr_chg,
    (sum(on_cost)/sum(on_clicks))/(sum(off_cost)/sum(off_clicks)) - 1 as cpc_chg,
    (sum(on_clicks)/sum(on_impressions))/(sum(off_clicks)/sum(off_impressions)) - 1 as ctr_chg,
    (sum(on_purchases)/sum(on_clicks))/(sum(off_purchases)/sum(off_clicks)) - 1 as pccvr_chg,
    (sum(on_purchase_amt)/sum(on_cost))/(sum(off_purchase_amt)/sum(off_cost)) - 1 as roas_chg,
    sum(on_impressions) as on_impressions,
    sum(on_purchases) as on_purchases,
    sum(off_impressions) as off_impressions,
    sum(off_purchases) as off_purchases,
    sum(on_clicks) as on_clicks,
    sum(off_clicks) as off_clicks
from 
    treatment_base a join
    control_base b on a.page_type = b.page_type and a.platform = b.platform and a.listing_id = b.listing_id
group by 1,2
order by 1,2,3
;

-- right offset
with treatment_base as (
select 
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    -- position,
    -- position,
    listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as on_visit_count,
    count(*) as on_impressions,
    sum(click) as on_clicks,
    sum(purchase) as on_purchases,
    sum(cost) as on_cost,
    sum(revenue) as on_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where 
    -- experiment
    large_ads_experiment = 1 and 
    -- pages
    ((desktop_search_ab_variant = 1 and page_type = 0) or (desktop_market_ab_variant = 1 and page_type = 1)
    or (mweb_search_ab_variant = 1 and page_type = 0) or (mweb_market_ab_variant = 1 and page_type = 1))
    and desktop_market_ab_variant_detail= 'right' 
    -- ad treatment type (if treatment)
    and specialadtreatments like '%offset%'
    -- position (if the treatment was only in a specific position)
    and position = 8
group by 1,2,3
),control_base as (
select 
    page_type,
    case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
    -- position,
    -- position,
    listing_id,
    -- visit_id,
    -- count(distinct visit_id) as visit_count,
    count(distinct visit_id) as off_visit_count,
    count(*) as off_impressions,
    sum(click) as off_clicks,
    sum(purchase) as off_purchases,
    sum(cost) as off_cost,
    sum(revenue) as off_purchase_amt
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where 
    -- experiment
    large_ads_experiment = 1 and 
    -- pages
    ((desktop_search_ab_variant = 0 and page_type = 0) or (desktop_market_ab_variant = 0 and page_type = 1)
    or (mweb_search_ab_variant = 0 and page_type = 0) or (mweb_market_ab_variant = 0 and page_type = 1)) 
    -- ad treatment type (if treatment)
    -- position (if the treatment was only in a specific position)
    and position = 8
group by 1,2,3
)
select
    a.platform,
    case when a.page_type = 0 then 'search' else 'market' end as page,
    -- a.position,
    sum(a.on_visit_count) as on_visit_count,
    sum(b.off_visit_count) as off_visit_count,
    -- count(distinct a.listing_id) as on_listing_count,
    -- count(distinct b.listing_id) as off_listing_count,
    (sum(on_impressions)/sum(on_visit_count))/(sum(off_impressions)/sum(off_visit_count)) - 1 as impr_chg,
    (sum(on_cost)/sum(on_clicks))/(sum(off_cost)/sum(off_clicks)) - 1 as cpc_chg,
    (sum(on_clicks)/sum(on_impressions))/(sum(off_clicks)/sum(off_impressions)) - 1 as ctr_chg,
    (sum(on_purchases)/sum(on_clicks))/(sum(off_purchases)/sum(off_clicks)) - 1 as pccvr_chg,
    (sum(on_purchase_amt)/sum(on_cost))/(sum(off_purchase_amt)/sum(off_cost)) - 1 as roas_chg,
    sum(on_impressions) as on_impressions,
    sum(on_purchases) as on_purchases,
    sum(off_impressions) as off_impressions,
    sum(off_purchases) as off_purchases,
    sum(a.on_clicks) as on_clicks,
    sum(b.off_clicks) as off_clicks
from 
    treatment_base a join
    control_base b on a.page_type = b.page_type and a.platform = b.platform and a.listing_id = b.listing_id
group by 1,2
order by 1,2,3
;
-- click share by variant
select 
    nat_test,
    case when page_type = 0 then 'search' else 'market' end as page,
    case when desktop_search_ab_variant = 1 or desktop_market_ab_variant = 1 then 'desktop' else 'mweb' end as platform,
    count(case when specialadtreatments != '' then visit_id end)/count(visit_id) as special_ad_share,
    count(case when specialadtreatments is not null then visit_id end)/count(visit_id) as null_ad_share
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where
    click = 1 and
    -- (desktop_search_ab_variant = 1 or desktop_market_ab_variant = 1 or mweb_search_ab_variant = 1 or mweb_market_ab_variant = 1)
    ((desktop_search_ab_variant = 1 and page_type = 0) or (desktop_market_ab_variant = 1 and page_type = 1)
    or (mweb_search_ab_variant = 1 and page_type = 0) or (mweb_market_ab_variant = 1 and page_type = 1))
group by 1,2,3
;


-- click heat map
with eligible_visits as (
    select 
        visit_id,
        nat_test,
        case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
        page_type,
        max(case when specialadtreatments is not null then 1 else 0 end) as special_ad
    from 
        `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
    where 
    analysis_flag = 1 and
    ((desktop_search_ab_variant = 1 and page_type = 0) or (desktop_market_ab_variant = 1 and page_type = 1)
    or (mweb_search_ab_variant = 1 and page_type = 0) or (mweb_market_ab_variant = 1 and page_type = 1)) 
    -- ad treatment type (if treatment)
    and ((specialadtreatments = 'zooming' and zooming_ads_experiment = 1) or (specialadtreatments like '%video%' and hover_to_play_experiment = 1) 
    or (specialadtreatments = 'multi-listing-ad' and multi_listing_ads_experiment = 1) or (specialadtreatments = 'multi-image-ad' and multi_image_ads_experiment = 1) or
    (specialadtreatments like '%offset%' and large_ads_experiment = 1))
    group by 1,2,3,4
    -- having special_ad = 1
),on_clicks as (
    select
        a.nat_test,
        a.position,
        case when a.page_type = 0 then 'search' else 'market' end as page,
        case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
        count(*) as on_impressions,
        sum(click) as on_clicks
    from 
        `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments` a join 
        eligible_visits b on a.nat_test = b.nat_test and a.visit_id = b.visit_id and a.nat_test = b.nat_test
    group by 1,2,3,4 
),off_clicks as (
    select 
        nat_test,
        case when page_type = 0 then 'search' else 'market' end as page,
        case when desktop_search_ab_variant is not null or desktop_market_ab_variant is not null then 'desktop' else 'mweb' end as platform,
        position,
        count(*) as off_impressions,
        sum(click) as off_clicks
    from
        `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
    where 
        ((desktop_search_ab_variant = 0 and page_type = 0) or (desktop_market_ab_variant = 0 and page_type = 1)
        or (mweb_search_ab_variant = 0 and page_type = 0) or (mweb_market_ab_variant = 0 and page_type = 1)) 
    group by 1,2,3,4
)
select 
    a.position,
    a.nat_test,
    a.platform,
    a.page,
    sum(on_impressions)/sum(sum(on_impressions)) over(partition by a.nat_test,a.platform,a.page) as on_imp_share,
    sum(off_impressions)/sum(sum(off_impressions)) over(partition by a.nat_test,a.platform,a.page) as off_imp_share,
    sum(on_clicks)/sum(sum(on_clicks)) over(partition by a.nat_test,a.platform,a.page) as on_click_share,
    sum(off_clicks)/sum(sum(off_clicks)) over(partition by a.nat_test,a.platform,a.page) as off_click_share,
    sum(on_clicks)/sum(on_impressions) as on_ctr,
    sum(off_clicks)/sum(off_impressions) as off_ctr,
    sum(on_impressions) as on_impressions,
    sum(off_impressions) as off_impressions
from 
    on_clicks a join
    off_clicks b on a.nat_test = b.nat_test and a.platform = b.platform and a.position = b.position and a.page = b.page
-- where 
--     position <= 10
group by 1,2,3,4
order by 2,3,4,1
;

-- proportion testing
-- share of special ads

-- share of orders with multiple transactions
with base as (
    select
        visit_id,
        nat_test,
        a.logging_key,
        a.receipt_id,
        count(distinct transaction_id) as transaction_count,
        sum(usd_subtotal_price)/sum(quantity) as aiv,
        sum(quantity) as total_quantity
    from
        `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments` a left join 
        `etsy-data-warehouse-prod.transaction_mart.all_transactions` b on a.receipt_id = b.receipt_id
    group by 1,2
),table_join as (
    select 
        a.*,
        transaction_count,
        aiv,
        total_quantity
)
select 
    desktop_market_ab_variant,
    sum(case when purchase = 1 then revenue end)/count(case when purchase = 1 then visit_id end) as aov
from 
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where 
    nat_test = 'triplet' and analysis_flag = 1
group by 1
order by 1
;
select 
    count(*) as impression_count,
    count(case when purchase = 1 then logging_key end) as purchases
from
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where
    multi_listing_ads_experiment = 1
select
    visit_id,
    desktop_market_ab_variant,
    listing_id,
    click, 
    purchase,
    revenue,
    receipt_id
from
    `etsy-data-warehouse-dev.pdavidoff.new_ad_treatments`
where
    multi_listing_ads_experiment = 1 and purchase = 1
limit 50
; 

-- what's up with ROAS?
with base as (
    select 

    from
)