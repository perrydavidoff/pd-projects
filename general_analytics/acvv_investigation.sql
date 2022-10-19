create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
	as (
with trans_visits as (
select
	distinct
	a._date as date,
	a.visit_id,
	a.browser_id,
	a.user_id,
	a.pages_seen,
	a.platform,
	a.visit_length,
	a.region,
	a.new_visitor,
	a.top_channel,
	b.receipt_id,
	max(case when d.is_gift = 1 then 1 else 0 end) as is_gift,
	max(case when d.personalization_request != "" then 1 else 0 end) as is_personalized,
	count(distinct b.transaction_id) as trans_count,
	sum(b.quantity) as total_quantity
from
	`etsy-data-warehouse-prod.weblog.recent_visits` a join
	`etsy-data-warehouse-prod.visit_mart.visits_transactions` b on a.visit_id = b.visit_id and date_diff(current_date(),b._date,day) <= 30 join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions_categories` c on b.transaction_id = c.transaction_id join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` d on b.transaction_id = d.transaction_id and date_diff(current_date(),d.date,day) <= 30
where
	date_sub(current_date(),interval 30 day) <= a._date
	and b.gms_net > 0 and 
	b.transaction_id not in (select transaction_id from `etsy-data-warehouse-prod.rollups.covid_transactions`)
group by 1,2,3,4,5,6,7,8,9,10,11
),receipts as (
select
	a.*,
	b.buyer_country_name,
	b.seller_country_name,
	b.transaction_count,
	b.has_digital,
	b.is_gift_card,
	b.top_category_items,
	b.top_category_gms,
	b.gms_net,
	b.gmv,
	b.gmv - gms_net as ship_cost,
	c.shipping_cost_segment,
	c.had_free_shipping,
	c.min_processing_days,
	c.max_processing_days,
	case when d.gift_purchase_propensity > 0.5 then 1 else 0 end as is_gift_prop
from
	trans_visits a join
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on a.receipt_id = b.receipt_id and date_diff(current_date(),extract(date from b.creation_tsz),day) <= 30 join
	`etsy-data-warehouse-prod.rollups.receipt_shipping_basics` c on a.receipt_id = c.receipt_id and date_diff(current_date(),extract(date from c.order_tsz),day) <= 30 left join
	`etsy-data-warehouse-prod.rollups.dbmarket_gift_receipts` d on a.receipt_id = d.receipt_id and date_diff(current_date(),d.purch_date,day) <= 30
)
select
	a.*,
	c.mapped_user_id,
	c.buyer_segment,
	b.join_date as up_join_date,
	c.join_date as mup_join_date,
	c.gender,
	c.is_guest,
	c.first_visit_top_channel,
	c.first_conv_visit_top_channel,
	d.estimated_age
from
	receipts a left join
	`etsy-data-warehouse-prod.user_mart.user_profile` b on a.user_id = b.user_id left join
	`etsy-data-warehouse-prod.user_mart.mapped_user_profile` c on b.mapped_user_id = c.mapped_user_id left join
	`etsy-data-warehouse-prod.rollups.buyer_basics` d on c.mapped_user_id = d.mapped_user_id
)
;

create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.acvv_over_time`
	as (
with receipts as (
select
	
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms`
where

select
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
;
-- ACVV trends
-- first, let's take a look at platform. looks like acvv is highest on desktop,
-- driven by higher quantity. boe, driven by a higher share of habitual buyers,
-- have high orders per visit and trans per order.

select
	platform,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
where
	platform not in ("soe","undefined")
group by 1
order by 2 desc
;


-- next, let's look at buyer segment. join with daily buyer segments table to get segment from day
-- before purchase. Less active buyers have higher ACVV than more experienced Etsy buyers.
-- Signed out buyers have significantly lower ACVV than signed in users
with base as (
select
	a.*,
	case 
		when a.user_id is null then "Signed Out"
		when b.buyer_segment is null and a.user_id is not null then "New" 
		else b.buyer_segment 
	end as pre_purchase_buyer_segment	
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends` a left join
	`etsy-data-warehouse-prod.catapult.catapult_daily_buyer_segments` b on a.user_id = b.user_id and a.date = b._date
)
select
	pre_purchase_buyer_segment,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	base
group by 1
order by 2 desc
;

-- now, look at category. we'll use GMS for this one.
-- amongst top 10 categories, home and living has the highest ACVV, driven by high AIV.
select
	top_category_gms,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;

-- acvv by personalization status
select
	is_personalized,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;



-- personalization and category breakout
select
	is_personalized,
	top_category_gms,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1,2
order by 3 desc
;



select
	count(distinct receipt_id) as receipt_count,
	count(distinct case when gift_purchase_propensity > 0.5 then receipt_id end)/count(distinct receipt_id) as gift_share	
from
	`etsy-data-warehouse-prod.rollups.dbmarket_gift_receipts` d
where
	date_diff(current_date(),d.purch_date,day) <= 30
;

select
	count(distinct receipt_id)
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms`
where
	date_diff(current_date(),extract(date from creation_tsz),day) <= 30
;


-- gift status
select
	is_gift,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;

-- gift and category break out
select
	is_gift,
	top_category_gms,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1,2
order by 3 desc
;

-- how about top channel? this will likely track with buyer segment
-- channels we own drive the highest ACVV
select
	top_channel,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;

-- where did we get the buyers from?
select
	first_visit_top_channel,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;

-- ship cost segment view
-- highest ACVV goes to items that have high relative ship costs. probably because this is 
-- when buyers can bear higher ship costs
select
	shipping_cost_segment,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,	
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;	

-- buyer country
-- US has the highest ACVV of all markets
select
	case when buyer_country_name in ("France","United States","United Kingdom","Australia","Germany","India","Canada") then buyer_country_name else "ROW" end as buyer_country,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;	


-- seller country
select
	case when seller_country_name in ("France","United States","United Kingdom","Australia","Germany","India","Canada") then seller_country_name else "ROW" end as seller_country,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;	

-- domestic vs. cross-border
select
	case when buyer_country_name = seller_country_name then "Domestic" else "Cross-Border" end as trade_route,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;	

-- gender
select
	case when gender in ("male","female") then gender else "other" end as gender,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 2 desc
;

-- account age
select
	extract(year from timestamp_seconds(join_date)) as join_year,
	sum(gms_net) as gms,
	sum(gms_net)/sum(sum(gms_net)) over() as gms_share,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
order by 1
;

-- acvv bucket
-- 25% of our GMS goes to buyers who spend $200 or more during their visit
with base as (
select
	visit_id,
	sum(gms_net) as gms,
	sum(gmv)/count(distinct visit_id) as acvgmv,
	sum(ship_cost)/count(distinct visit_id) as avg_shipping,
	sum(gms_net)/count(distinct visit_id) as acvv,
	sum(gms_net)/count(distinct receipt_id) as aov,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	`etsy-data-warehouse-dev.pdavidoff.acvv_trends`
group by 1
)
select
	case when round(acvv/10)*10 < 200 then round(acvv/10)*10 else 200 end as acvv_groups,
	count(distinct visit_id)/sum(count(distinct visit_id)) over() as converting_visit_share,
	sum(gms)/sum(sum(gms)) over() as gms_share
from
	base
group by 1
order by 1
;


-- buyer number trends
with base as (
select
	extract(date from a.creation_tsz) as date,
	a.buyer_user_id,
	a.receipt_id,
	gms_net,
	-- row_number() over(partition by buyer_user_id order by extract(date from creation_tsz)) as row,
	sum(quantity) as total_quantity,
	count(distinct transaction_id) as trans_count
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` a join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` b on a.receipt_id = b.receipt_id
where
	a.creation_tsz >= "2013-01-01"
group by 1,2,3,4
),base2 as (
select
	*,
	row_number() over(partition by buyer_user_id order by date) as purchase_count
from
	base
),first_purchase as (
    select 
        a.*,
        extract(year from first_purchase_date) as first_purchase_year
    from 
        base2 a join
        `etsy-data-warehouse-prod.user_mart.user_profile` b on a.buyer_user_id = b.user_id join 
        `etsy-data-warehouse-prod.rollups.buyer_basics` c on b.mapped_user_id = c.mapped_user_id
)
select
    first_purchase_year,
	purchase_count,
    count(distinct buyer_user_id) as buyer_count,
	sum(gms_net)/count(distinct receipt_id) as aov,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	first_purchase
where
    purchase_count <= 20 and first_purchase_year >= 2013
group by 1,2
order by 1,2
;




-- buyer number trends within a year of first purchase
with base as (
select
	extract(date from a.creation_tsz) as date,
	a.buyer_user_id,
	a.receipt_id,
	gms_net,
	-- row_number() over(partition by buyer_user_id order by extract(date from creation_tsz)) as row,
	sum(quantity) as total_quantity,
	count(distinct transaction_id) as trans_count
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` a join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` b on a.receipt_id = b.receipt_id
where
	a.creation_tsz >= "2013-01-01"
group by 1,2,3,4
),base2 as (
select
	*,
	row_number() over(partition by buyer_user_id order by date) as purchase_count
from
	base
),first_purchase as (
    select 
        a.*,
        first_purchase_date,
        extract(year from first_purchase_date) as first_purchase_year
    from 
        base2 a join
        `etsy-data-warehouse-prod.user_mart.user_profile` b on a.buyer_user_id = b.user_id join 
        `etsy-data-warehouse-prod.rollups.buyer_basics` c on b.mapped_user_id = c.mapped_user_id
)
select
    first_purchase_year,
	purchase_count,
    count(distinct buyer_user_id) as buyer_count,
	sum(gms_net)/count(distinct receipt_id) as aov,
	sum(trans_count)/count(distinct receipt_id) as trans_per_order,
	sum(total_quantity)/sum(trans_count) as quantity_per_item,
	sum(gms_net)/sum(total_quantity) as aiv
from
	first_purchase
where
    purchase_count <= 20 and first_purchase_year >= 2013 and 
    date_diff(date,first_purchase_date,day) between 0 and 365
group by 1,2
order by 1,2
;


