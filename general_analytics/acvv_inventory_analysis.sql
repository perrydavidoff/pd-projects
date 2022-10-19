-- Title: ACVV analysis into inventory gaps
-- Date: December, 2021
-- Overview: Analyze price diversity in shops and buyer signals about the prices they're willing
-- to spend on items

-- first, where do multi-item/multi-quantity purchases come from? same shop? multi shop?
with base as (
select
	a.receipt_group_id,
	count(distinct b.receipt_id) as receipt_count,
	count(distinct transaction_id) as transaction_count,
	sum(quantity) as total_quantity
from
	`etsy-data-warehouse-prod.etsy_shard.user_receipt_groups` a join
	`etsy-data-warehouse-prod.transaction_mart.all_receipts` b on a.receipt_group_id = b.receipt_group_id join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` c on b.receipt_id = c.receipt_id
where
	extract(date from (timestamp_seconds(create_date))) >= current_date - 120
group by 1
)
select
	count(distinct case when total_quantity > 1 then receipt_group_id end)/count(distinct receipt_group_id) as multi_item_rate,
	count(distinct case when total_quantity > 1 and receipt_count > 1 then receipt_group_id end)/count(distinct receipt_group_id) as multi_item_multi_shop_rate,
	count(distinct case when total_quantity > 1 and receipt_count = 1 then receipt_group_id end)/count(distinct receipt_group_id) as multi_item_same_shop_rate,
	count(distinct case when transaction_count > 1 then receipt_group_id end)/count(distinct receipt_group_id) as multi_trans_rate,
	count(distinct case when transaction_count > 1 and receipt_count > 1 then receipt_group_id end)/count(distinct receipt_group_id) as multi_trans_multi_shop_rate,
	count(distinct case when transaction_count > 1 and receipt_count = 1 then receipt_group_id end)/count(distinct receipt_group_id) as multi_trans_same_shop_rate
from
	base
;

-- show the overall numbers
select
	sum(trans_gms_gross)/count(distinct visit_id) as acvv,
	count(distinct receipt_id)/count(distinct visit_id) as orders_per_visit,
	sum(trans_gms_gross)/count(distinct receipt_id) as aov,
	sum(trans_gms_gross)/sum(quantity) as aiv,
	sum(quantity)/count(distinct transaction_id) as quantity_per_item,
	count(distinct transaction_id)/count(distinct receipt_id) as items_per_order
from
	`etsy-data-warehouse-prod.visit_mart.visits_transactions`
where
	_date >= current_date - 30
;

-- how have the different categories of orders changed in terms of basket value over time?
-- share of views, share of transactions, share of GMS by dollar bin
-- 50% of GMS goes to single item, single shop orders
-- let's look at how ACVV has changed over the past year

with item_base as (
select
	distinct
	date_trunc(a.date,week) as week,
	c.receipt_group_id,
	a.receipt_id,
	a.transaction_id,
	a.usd_price,
	a.quantity,
	ntile(10) over(order by usd_price) as item_price_decile
from
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` a join
	`etsy-data-warehouse-prod.transaction_mart.all_receipts` b on a.receipt_id = b.receipt_id and extract(date from b.creation_tsz) >= "2017-01-01" join
	`etsy-data-warehouse-prod.etsy_shard.user_receipt_groups` c on b.receipt_group_id = c.receipt_group_id and extract(date from timestamp_seconds(create_date)) >= "2017-01-01"
where
	date >= "2017-01-01"
),receipt_level as (
select
	week,
	receipt_group_id,
	count(distinct transaction_id) as transaction_count,
	sum(quantity) as item_count
from
	item_base
group by 1,2
),receipt_join as (
select
	week,
	a.receipt_group_id,
	transaction_count,
	item_count,
	count(distinct b.receipt_id) as receipt_count,
	sum(receipt_usd_subtotal_price) as receipt_group_gmv,
	sum(receipt_usd_total_price) as receipt_group_gms,
from
	receipt_level a join
	`etsy-data-warehouse-prod.transaction_mart.all_receipts` b on a.receipt_group_id = b.receipt_group_id and extract(date from creation_tsz) >= "2017-01-01"
group by 1,2,3,4
)
select
	week,
	avg(case when item_count = 1 then receipt_group_gms end) as single_item_aov,
	avg(case when item_count > 1 and transaction_count = 1 and receipt_count = 1 then receipt_group_gms end) as multi_quant_single_item_single_shop_aov,
	avg(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count = 1 and receipt_count = 1 then receipt_group_gms end) as multi_item_single_shop_aov,
	avg(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count = 1 and receipt_count > 1 then receipt_group_gms end) as multi_item_multi_shop_aov,
	avg(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count != 1 and receipt_count = 1 then receipt_group_gms end) as multi_quant_multi_item_single_shop_aov,
	avg(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count != 1 and receipt_count > 1 then receipt_group_gms end) as multi_quant_multi_item_multi_shop_aov,
	avg(case when item_count > 1 then receipt_group_gms end) as multi_item_aov,
	sum(case when item_count = 1 then receipt_group_gms end)/sum(sum(receipt_group_gms)) over(partition by week) as single_item_gms_share,
	sum(case when item_count > 1 and transaction_count = 1 and receipt_count = 1 then receipt_group_gms end)/sum(sum(receipt_group_gms)) over(partition by week) as multi_quant_single_item_single_shop_gms_share,
	sum(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count = 1 and receipt_count = 1 then receipt_group_gms end)/sum(sum(receipt_group_gms)) over(partition by week) as multi_item_single_shop_gms_share,
	sum(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count = 1 and receipt_count > 1 then receipt_group_gms end)/sum(sum(receipt_group_gms)) over(partition by week) as multi_item_multi_shop_gms_share,
	sum(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count != 1 and receipt_count = 1 then receipt_group_gms end)/sum(sum(receipt_group_gms)) over(partition by week) as multi_quant_multi_item_single_shop_gms_share,
	sum(case when item_count > 1 and transaction_count > 1 and transaction_count/item_count != 1 and receipt_count > 1 then receipt_group_gms end)/sum(sum(receipt_group_gms)) over(partition by week) as multi_quant_multi_item_multi_shop_gms_share,
	count(distinct case when item_count = 1 then receipt_group_id end)/sum(count(distinct receipt_group_id)) over(partition by week) as single_item_order_share,
	count(distinct case when item_count > 1 and transaction_count = 1 and receipt_count = 1 then receipt_group_id end)/sum(count(distinct receipt_group_id)) over(partition by week) as multi_quant_single_item_single_shop_order_share,
	count(distinct case when item_count > 1 and transaction_count > 1 and transaction_count/item_count = 1 and receipt_count = 1 then receipt_group_id end)/sum(count(distinct receipt_group_id)) over(partition by week) as multi_item_single_shop_order_share,
	count(distinct case when item_count > 1 and transaction_count > 1 and transaction_count/item_count = 1 and receipt_count > 1 then receipt_group_id end)/sum(count(distinct receipt_group_id)) over(partition by week) as multi_item_multi_shop_order_share,
	count(distinct case when item_count > 1 and transaction_count > 1 and transaction_count/item_count != 1 and receipt_count = 1 then receipt_group_id end)/sum(count(distinct receipt_group_id)) over(partition by week) as multi_quant_multi_item_single_shop_order_share,
	count(distinct case when item_count > 1 and transaction_count > 1 and transaction_count/item_count != 1 and receipt_count > 1 then receipt_group_id end)/sum(count(distinct receipt_group_id)) over(partition by week) as multi_quant_multi_item_multi_shop_order_share
from
	receipt_join
group by 1
order by 1 desc
;

-- what is the price distribution of single item/single shop orders?

select
	a.receipt_id,
	top_category_gms,
	count(distinct transaction_id) as transaction_count,
	sum(quantity) as total_quantity,
	max(b.gms_net) as aiv
from
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` a join
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on a.receipt_id = b.receipt_id
where
	extract(date from a.creation_tsz) >= current_date - 90
group by 1,2
having count(distinct transaction_id) = 1 and sum(quantity) = 1
order by rand()
limit 1000000
;

-- what do multi-item/multi-quantity purchases look like?
-- break out of multi vs. single, single vs. multi, price of main vs. additional
with item_base as (
select
	distinct
	c.receipt_group_id,
	a.receipt_id,
	a.transaction_id,
	a.usd_price,
	a.quantity,
	ntile(10) over(order by usd_price) as item_price_decile
from
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` a join
	`etsy-data-warehouse-prod.transaction_mart.all_receipts` b on a.receipt_id = b.receipt_id and extract(date from b.creation_tsz) >= current_date - 30 join
	`etsy-data-warehouse-prod.etsy_shard.user_receipt_groups` c on b.receipt_group_id = c.receipt_group_id and extract(date from timestamp_seconds(create_date)) >= current_date - 30
where
	date >= current_date - 30
),receipt_level as (
select
	receipt_group_id,
	-- receipt_id,
	-- count(distinct receipt_id) as receipt_count,
	count(distinct transaction_id) as transaction_count,
	sum(quantity) as item_count,
	max(usd_price) as highest_priced_item,
	min(usd_price) as lowest_priced_item,
	avg(usd_price) as average_price,
	max(item_price_decile) as highest_item_price_decile,
	min(item_price_decile) as lowest_item_price_decile,
	avg(item_price_decile) as average_item_price_decile
from
	item_base
group by 1
),receipt_join as (
select
	a.receipt_group_id,
	transaction_count,
	item_count,
	highest_priced_item,
	lowest_priced_item,
	average_price,
	highest_item_price_decile,
	lowest_item_price_decile,
	average_item_price_decile,
	count(distinct b.receipt_id) as receipt_count,
	sum(receipt_usd_subtotal_price) as receipt_group_gmv,
	sum(receipt_usd_total_price) as receipt_group_gms,
from
	receipt_level a join
	`etsy-data-warehouse-prod.transaction_mart.all_receipts` b on a.receipt_group_id = b.receipt_group_id and extract(date from creation_tsz) >= current_date - 30
group by 1,2,3,4,5,6,7,8,9
)
select
	case when item_count = 1 then "Single Item" else "Multi Item" end as item_group,
	case when transaction_count = 1 then "Single Listing" else "Multi Listing" end as lising_group,
	case when receipt_count = 1 then "Single Shop" else "Multi Shop" end as shop_group,
	case 
		when highest_item_price_decile between 8 and 10 then "High" 
		when highest_item_price_decile between 1 and 3 then "Low"
		else "Middle"
	end as highest_item_price_group,
	case 
		when lowest_item_price_decile between 8 and 10 then "High" 
		when lowest_item_price_decile between 1 and 3 then "Low"
		else "Middle"
	end as lowest_item_price_group,
	case 
		when average_item_price_decile between 8 and 10 then "High" 
		when average_item_price_decile between 1 and 3 then "Low"
		else "Middle"
	end as avg_item_price_group,
	sum(receipt_group_gms)/count(*) as avg_basket_value,
	sum(receipt_group_gms)/sum(item_count) as aiv,
	sum(item_count)/count(*) as items_per_receipt_group,	
	count(*) as receipt_groups,	
	sum(receipt_group_gms) as total_gms,
	count(*)/sum(count(*)) over() as receipt_group_pct,
	sum(receipt_group_gms)/sum(sum(receipt_group_gms)) over() as total_gms_pct
from
	receipt_join
group by 1,2,3,4,5,6
;

-- how have the prices of surviving listings changed over time?
-- looks like listings that were around have increased in line with inflation (~9%).
with base as (
select
	listing_id,
	min(date) as first_listing_week,
	max(date) as last_listing_week
from
	`etsy-data-warehouse-prod.incrementals.listing_daily`
group by 1
),base2 as (
select
	distinct
	b.date as week,
	a.listing_id,
	b.price_usd/100 as price_usd,
	percentile_cont(price_usd/100,0.25) over(partition by b.date) as quartile_1,
	percentile_cont(price_usd/100,0.5) over(partition by b.date) as median,
	percentile_cont(price_usd/100,0.75) over(partition by b.date) as quartile_3 
from
	base a join
	`etsy-data-warehouse-prod.incrementals.listing_daily` b on a.listing_id = b.listing_id and b.date >= "2018-12-30"
where
	first_listing_week <= "2019-01-01" and last_listing_week = "2021-12-12"
)
select
	week,
	quartile_1,
	median,
	quartile_3,
	avg(price_usd) as avg_price,
from
	base2
group by 1,2,3,4
order by 1
;


-- demand: Clicking on a listing that is higher priced than the result set
-- supply: inconsistencies between listing price shown and listing price inventory


-- clicks
-- create a dataset where there is a row for every click. we want to know what buyers click on
-- relative to the listing prices of the items they have available
-- supply and demand. classify queries in different groups based on their median price
create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.top_search_queries`
as (
select
	query,
	count(*) as total_sessions,
	sum(attributed_gms) as attributed_gms
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date >= "2021-11-25"
group by 1
order by 3 desc
limit 1000000
)
;


-- there are only 30 days in the visit_level_listing_impressions table. don't re-run this or will have to reset much of this analysis
-- create or replace table
--   `etsy-data-warehouse-dev.pdavidoff.aov_quartiles_ranges`
--   as (
  	with query_base as (
  		select
  			a.visit_id,
  			a.query,
  			a.listing_id
  		from
  			(select distinct visit_id,query,listing_id from `etsy-data-warehouse-prod.search.visit_level_listing_impressions` where _date between "2021-11-28" and "2021-12-27" and page = "search" order by query) a join
  			(select query from `etsy-data-warehouse-dev.pdavidoff.top_search_queries` order by query) b on a.query = b.query
  		),aov_supply as (
  		select
			distinct
			a.visit_id,
			a.query,
			a.listing_id,
			b.price_usd/100 as price_usd,
			c.user_id,
			case 
				when c.user_id is not null and d.user_id is null then "New"
				when c.user_id is null then "Signed Out"
				else buyer_segment
			end as buyer_segment
	from
		query_base a join
		`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = b.listing_id join
		`etsy-data-warehouse-prod.weblog.recent_visits` c on a.visit_id = c.visit_id and c.platform in ("mobile_web","desktop") and c._date between "2021-11-28" and "2021-12-27" left join
		`etsy-data-warehouse-prod.catapult.catapult_daily_buyer_segments` d on c.user_id = d.user_id and c._date = d._date and d._date between "2021-11-28" and "2021-12-27"
	)
	select
		*,
		row_number() over(partition by visit_id,query order by price_usd) as price_row
		-- percentile_cont(price_usd,0.25) over(partition by visit_id,query) as imp_price_quartile_1,
		-- percentile_cont(price_usd,0.5) over(partition by visit_id,query) as imp_price_median,
		-- percentile_cont(price_usd,0.75) over(partition by visit_id,query) as imp_price_quartile_3
	from
		aov_supply
	)
	;

create or replace table
	`etsy-data-warehouse-dev.pdavidoff.listing_characteristics`
	as (
		with listing_base as (
			select
				a.*,
				b.shop_id,
				e.seller_tier,
				c.is_personalizable,
				c.is_digital,
				d.favorite_count,
				d.tag_count,
				d.image_count,
				extract(date from timestamp_seconds(b.original_create_date)) as original_create_date,
				last_star_seller_tier,
				accepts_returns,
				buyer_promise_enabled
			from
				(select * from `etsy-data-warehouse-dev.pdavidoff.aov_quartiles_ranges` order by listing_id) a left join
				`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = b.listing_id left join
				`etsy-data-warehouse-prod.listing_mart.listing_attributes` c on b.listing_id = c.listing_id left join
				`etsy-data-warehouse-prod.listing_mart.listing_counts` d on b.listing_id = d.listing_id left join
				`etsy-data-warehouse-prod.rollups.seller_basics` e on b.shop_id = e.shop_id
			order by listing_id
		),listing_giftiness as (
		select
			listing_id,
			avg(overall_giftiness) as giftiness_score
		from
			`etsy-data-warehouse-prod.knowledge_base.listing_giftiness`
		where
			_date between "2021-11-28" and "2021-12-27"
		group by 1
		order by 1
		),listing_reviews as (
		select
			listing_id,
			avg(rating) as avg_listing_rating,
			count(listing_review_id) as total_listing_reviews
		from
			`etsy-data-warehouse-prod.etsy_shard.listing_review`
		where	
			extract(date from timestamp_seconds(create_date)) >= "2021-11-28" - 365
		group by 1
		order by 1
		),shop_reviews as (
		select
			shop_id,
			count(listing_review_id) as total_shop_reviews,
			avg(rating) as avg_shop_rating
		from
			`etsy-data-warehouse-prod.etsy_shard.listing_review`
		where
			extract(date from timestamp_seconds(create_date)) >= "2021-11-28" - 365
		group by 1
		order by 1
		),hobbies as (
		select
			listing_id,
			display_name,
			avg(score) as avg_hobby_score
		from
			`etsy-data-warehouse-prod.knowledge_base.listing_concept_attrs`
		where
			_date between "2021-11-28" and "2021-12-27"
		group by 1,2
		),hobby_rank as (
		select
			listing_id,
			max(case when row = 1 then display_name end) as hobby_1,
			max(case when row = 1 then avg_hobby_score end) as hobby_1_score,
			max(case when row = 2 then display_name end) as hobby_2,
			max(case when row = 2 then avg_hobby_score end) as hobby_2_score,
			max(case when row = 3 then display_name end) as hobby_3,
			max(case when row = 3 then avg_hobby_score end) as hobby_3_score
		from
			(select listing_id,display_name,avg_hobby_score,row_number() over(partition by listing_id order by avg_hobby_score desc) as row from hobbies)
		where
			row in (1,2,3)
		group by 1
		)
		select
			a.*,
			b.avg_listing_rating,
			giftiness_score,
			b.total_listing_reviews,
			c.total_shop_reviews,
			c.avg_shop_rating,
			hobby_1,
			hobby_1_score,
			hobby_2,
			hobby_2_score,
			hobby_3,
			hobby_3_score
		from
			listing_base a left join
			listing_reviews b on a.listing_id = b.listing_id left join
			shop_reviews c on a.shop_id = c.shop_id left join
			listing_giftiness d on a.listing_id = d.listing_id left join
			hobby_rank e on a.listing_id = e.listing_id
	)
	;

-- put together table that categorizes queries into groups based on the median price and IQR
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.aov_supply_classify`
  as (
  with group_it as (
  	select
  		visit_id,
  		query,
  		buyer_segment,
  		approx_quantiles(price_usd,100)[OFFSET(25)] as imp_price_quartile_1,
		approx_quantiles(price_usd,100)[OFFSET(50)] as imp_price_median,
		approx_quantiles(price_usd,100)[OFFSET(75)] as imp_price_quartile_3
	from
		`etsy-data-warehouse-dev.pdavidoff.aov_quartiles_ranges`
	group by 1,2,3
  )
	select
		*,
		case 
			when ntile(10) over(order by imp_price_median) in (1,2) then "Very Low"
			when ntile(10) over(order by imp_price_median) in (3,4) then "Low"
			when ntile(10) over(order by imp_price_median) in (5,6) then "Medium"
			when ntile(10) over(order by imp_price_median) in (7,8) then "High"
			when ntile(10) over(order by imp_price_median) in (9,10) then "Very High"
		 end as aov_supply_price_group,
		 case
		 	when ntile(10) over(order by imp_price_quartile_3-imp_price_quartile_1) in (1,2) then "Very Low"
		 	when ntile(10) over(order by imp_price_quartile_3-imp_price_quartile_1) in (3,4) then "Low"
		 	when ntile(10) over(order by imp_price_quartile_3-imp_price_quartile_1) in (5,6) then "Medium"
		 	when ntile(10) over(order by imp_price_quartile_3-imp_price_quartile_1) in (7,8) then "High"
		 	when ntile(10) over(order by imp_price_quartile_3-imp_price_quartile_1) in (9,10) then "Very High"
		 end as aov_supply_variability_group
	from
		group_it
	)
	;




	create or replace table
  `etsy-data-warehouse-dev.pdavidoff.demand_data`
  as (
  with base as (
	select
		a.visit_id,
		case 
			when strpos(url,"&ref=")-strpos(url,"ga_search_query=")-16 > 0 then 
				replace(substr(url,strpos(url,"ga_search_query=")+16,strpos(url,"&ref=")-strpos(url,"ga_search_query=")-16),"+"," ")
			else "" 
		end as query,
		a.listing_id,
		-- case 
		-- 	when substr(a.ref_tag,1,3) = "sr_" then "Organic"
		-- 	when substr(a.ref_tag,1,3) = "sc_" then "Paid"
		-- 	else "Other"
		-- end as listing_type,
		min(case
			when substr(a.ref_tag,13,1) = "-" then substr(a.ref_tag,12,1) 
			else substr(a.ref_tag,12,2) 
		end) as page_no,
		min(case
			when substr(a.ref_tag,15,1) = "" then substr(a.ref_tag,14,1)
			else substr(a.ref_tag,14,2)
		end) as position,
		count(*) as listing_views,
		-- a.sequence_number,
		-- a.ref_tag,
		-- b.url,
		max(price_usd) as price_usd,
		max(added_to_cart) as added_to_cart,
		max(purchased_after_view) as purchased,
		max(image_count) as image_count,
		max(nudges_seen) as nudges_seen,
		max(text_reviews_seen) as text_reviews_seen,
		max(shop_rating_count) as shop_rating_count,
		max(listing_rating_count) as listing_rating_count,
		max(is_bestseller) as is_bestseller,
		max(sale_type) as sale_type,
		-- max(date_diff(cast(edd_shown_min as date),a._date,day)) as edd_shown_min,
		-- max(date_diff(cast(edd_shown_max as date),a._date,day)) as edd_shown_max,
		max(medd_framework_min_edd) as medd_framework_min_edd,
		max(medd_framework_max_edd) as medd_framework_max_edd,
		max(favorited) as favorited,
		max(detected_region) as detected_region
from
	`etsy-data-warehouse-prod.analytics.listing_views` a join
	`etsy-data-warehouse-prod.weblog.events` b on a.sequence_number = b.sequence_number and a.visit_id = b.visit_id and b._date >= current_date - 30
where
	a.platform in ("desktop","mobile_web") and a.referring_page_event = "search"
	and a._date >= current_date - 30 and a.ref_tag like "%gallery%"
group by 1,2,3
order by query
)
  select
  	a.*
  from
  	base a join
  	`etsy-data-warehouse-dev.pdavidoff.top_search_queries` b on a.query = b.query
)
;



	create or replace table
  `etsy-data-warehouse-dev.pdavidoff.aov_supply_classification`
  as (
select
	a.visit_id,
	a.query,
	a.buyer_segment,
	-- a.listing_id,
	-- a.price_usd,
	-- a.taxonomy_id,
	-- a.taxo_leaf_node,
	b.aov_supply_price_group,
	b.aov_supply_variability_group,
	b.imp_price_quartile_1,
	b.imp_price_median,
	b.imp_price_quartile_3,
	-- d.inference.label as query_group_label,
	-- sum(listing_views) as total_listing_views,
	-- sum(added_to_cart) as total_cart_adds,
	-- sum(purchased) as total_purchases,
	-- avg(image_count) as avg_image_count,
	-- avg(text_reviews_seen) as avg_text_reviews_seen,
	-- avg(shop_rating_count) as avg_shop_rating_count,
	-- avg(listing_rating_count) as avg_listing_rating_count,
	avg(case when seller_tier in ("top seller","power seller") then 1 else 0 end) as top_seller_share,
	avg(case when last_star_seller_tier = "star_seller" then 1 else 0 end) as star_seller_share,
	avg(coalesce(is_personalizable,0)) as personalizable_share,
	avg(coalesce(is_digital,0)) as digital_share,
	avg(coalesce(favorite_count,0)) as avg_favorite_count,
	avg(coalesce(tag_count,0)) as avg_tag_count,
	avg(coalesce(a.image_count,0)) as avg_image_count,
	avg(coalesce(accepts_returns,0)) as accepts_return_share,
	avg(coalesce(buyer_promise_enabled,0)) as bp_enabled_share,
	avg(coalesce(total_listing_reviews,0)) as avg_past_year_listing_reviews,
	avg(coalesce(total_shop_reviews,0)) as avg_past_year_shop_reviews,
	avg(avg_listing_rating) as avg_listing_rating,
	avg(avg_shop_rating) as avg_shop_rating,
	avg(coalesce(hobby_1_score)) as avg_hobby_score,
	avg(coalesce(giftiness_score)) as avg_giftiness_score,
	avg(case when is_bestseller is true then 1 else 0 end) as avg_bestseller,
	avg(case when sale_type is not null then 1 else 0 end) as sale_rate,
	-- avg(edd_shown_min) as avg_shown_edd_min,
	-- avg(edd_shown_max) as avg_shown_edd_max,
	max(medd_framework_min_edd) as medd_framework_min_edd,
	max(medd_framework_max_edd) as medd_framework_max_edd,
	avg(favorited) as favorite_rate,
	max(detected_region) as detected_region,
	avg(case when c.listing_id is not null then a.price_row end) as avg_click_price_row,
	avg(case when c.added_to_cart = 1 then a.price_row end) as avg_atc_price_row,
	avg(case when c.purchased = 1 then a.price_row end) as avg_purchase_price_row,
	max(a.price_row) as listing_count,
	avg(case when c.listing_id is not null then a.price_usd end) as avg_click_price_usd,
	avg(case when c.added_to_cart = 1 then a.price_usd end) as avg_atc_price_usd,
	avg(case when c.purchased = 1 then a.price_usd end) as avg_purchase_price_usd
from
	`etsy-data-warehouse-dev.pdavidoff.listing_characteristics` a left join
	`etsy-data-warehouse-dev.pdavidoff.aov_supply_classify` b on a.visit_id = b.visit_id and a.query = b.query left join
	`etsy-data-warehouse-dev.pdavidoff.demand_data` c on a.visit_id = c.visit_id and a.query = c.query and a.listing_id = c.listing_id
	-- `etsy-data-warehouse-prod.arizona.query_intent_labels` d on a.query = d.query_raw
group by 1,2,3,4,5,6,7,8
)
;



create or replace table
  `etsy-data-warehouse-dev.pdavidoff.aov_query_intent_groups`
  as (
with base as (
select
	query_raw,
	inference.label as query_group_label,
	inference.confidence as query_group_label_conf_score,
	row_number() over(partition by query_raw order by inference.confidence desc) as rn
from
	`etsy-data-warehouse-prod.arizona.query_intent_labels`
),base2 as (
select
	a.*,
	b.query_group_label_conf_score,
	b.query_group_label
from
	`etsy-data-warehouse-dev.pdavidoff.aov_supply_classification` a left join
	base b on a.query = b.query_raw and b.rn = 1
),query_category as (
select
	date as visit_date,
	a.visit_id,
	a.query,
	max(from_autosuggest) as from_autosuggest,
	max(from_trending_search) as from_trending_search,
	max(max_total_results) as max_total_results,
	min(min_total_results) as min_total_results,
	max(b.classified_taxonomy_id) as classified_taxonomy_id,
	max(has_click) as search_attributed_click,
	max(has_cart) as search_attributed_cart_add,
	max(has_favorite) as search_attributed_favorite,
	max(attributed_gms) as search_attributed_gms,
	max(has_purchase) as search_attributed_purchase
from
	(select distinct visit_id,query from base2 order by visit_id,query) a left join
	(select distinct _date as date,visit_id,query,classified_taxonomy_id,max_total_results,min_total_results,from_autosuggest,from_trending_search,has_cart,has_favorite,has_purchase,attributed_gms,has_click from `etsy-data-warehouse-prod.search.query_sessions_new` where _date >= "2021-11-18" order by visit_id,query) b on a.visit_id = b.visit_id and a.query = b.query
group by 1,2,3
)
select
	a.*,
	visit_date,
	classified_taxonomy_id as query_taxonomy_id,
	(split(full_path, ".")[ORDINAL(1)]) as query_top_category,
	path as query_leaf_node,
	b.from_autosuggest,
	b.from_trending_search,
	b.max_total_results,
	b.min_total_results,
	b.classified_taxonomy_id,
	b.search_attributed_click,
	b.search_attributed_cart_add,
	b.search_attributed_favorite,
	b.search_attributed_gms,
	b.search_attributed_purchase
from
	base2 a left join
	query_category b on a.visit_id = b.visit_id and a.query = b.query left join
	-- `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` c on b.classified_taxonomy_id = c.taxonomy_id left join
	(select distinct taxonomy_id,path,full_path from `etsy-data-warehouse-prod.materialized.listing_taxonomy`) c on b.classified_taxonomy_id = c.taxonomy_id
)
;





-- join visit level purchases and long-tail of purchases
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
  as (
with visit_base as (
select
	(split(visit_id, ".")[ORDINAL(1)]) as browser_id,
	visit_id,
	new_category,
	receipt_id,
	_date as next_purchase_date,
	count(distinct transaction_id) as trans_count,
	max(gms_net) as aov,
	sum(gms_net)/sum(quantity) as aiv
from
	`etsy-data-warehouse-prod.visit_mart.visits_transactions`
where
	_date >= "2021-11-18"
group by 1,2,3,4,5
),base3 as (
select
	browser_id,
	visit_id as next_visit_id,
	next_purchase_date,
	receipt_id,
	aiv,
	aov,
	trans_count,
	max(new_category) as top_category
from
	(select *,row_number() over(partition by browser_id order by visit_id,receipt_id) as rn from visit_base)
where
	rn = 1
group by 1,2,3,4,5,6,7
)
select
	a.*,
	b.next_visit_id,
	b.next_purchase_date,
	b.top_category as next_visit_purchase_category,
	b.aov as next_visit_aov,
	b.aiv as next_purchase_aiv,
from
	`etsy-data-warehouse-dev.pdavidoff.aov_query_intent_groups` a left join
	base3 b on (split(a.visit_id, ".")[ORDINAL(1)]) = b.browser_id and b.next_visit_id > a.visit_id
)
;


-- first, look at the price ranges for the queries in this dataset.
-- the median price of items we show is ~$19.73. Q1 is $13.34 and Q3 is $28. relatively wide
-- range reflecting the wide range of queries we have. the median impression price is 
-- higher than the median AIV. 

select
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	count(*) as total_query_sessions,
	count(case when search_attributed_click > 0 then visit_id end)/count(visit_id) as search_attrib_lv,
	count(case when search_attributed_cart_add > 0 then visit_id end)/count(visit_id) as search_attrib_cart_add,
	count(case when search_attributed_purchase > 0 then visit_id end)/count(visit_id) as search_attrib_cr,
	sum(case when total_listing_views > 0 then avg_click_price_row end)/sum(case when total_listing_views > 0 then listing_count end) as avg_lv_price_location,
	sum(case when total_cart_adds > 0 then avg_click_price_row end)/sum(case when total_cart_adds > 0 then listing_count end) as avg_atc_price_location,
	sum(case when total_purchases > 0 then avg_click_price_row end)/sum(case when total_purchases > 0 then listing_count end) as avg_purchase_price_location
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
;



-- next, look at the different query groups we created
-- when grouping by supply group, we can see that click rates are higher at the AIV
-- extremes. they're highest when the AIV range is higher.
-- this seems to be driven by direct queries being in those groups
select
	aov_supply_price_group,
	-- aov_supply_variability_group,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	count(*) as total_query_sessions,
	count(case when search_attributed_click = 1 then visit_id end)/count(visit_id) as search_attrib_lv,
	count(case when search_attributed_cart_add = 1 then visit_id end)/count(visit_id) as search_attrib_cart_add,
	count(case when search_attributed_purchase = 1 then visit_id end)/count(visit_id) as search_attrib_cr,
	sum(search_attributed_gms/100)/count(visit_id) as gms_per_query,
	sum(case when total_listing_views > 0 then avg_click_price_row end)/sum(case when total_listing_views > 0 then listing_count end) as avg_view_price_location,
	sum(case when total_cart_adds > 0 then avg_click_price_row end)/sum(case when total_cart_adds > 0 then listing_count end) as avg_atc_price_location,
	sum(case when total_purchases > 0 then avg_click_price_row end)/sum(case when total_purchases > 0 then listing_count end) as avg_purchase_price_location
from
	`etsy-data-warehouse-dev.pdavidoff.aov_query_intent_groups`
where
	query_group_label = "Direct"
group by 1
order by 2
;

-- by broad vs. direct
select
	query_group_label,
	-- aov_supply_variability_group,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	count(*) as total_query_sessions,
	count(*)/sum(count(*)) over() as query_share,
	count(case when search_attributed_click = 1 then visit_id end)/count(visit_id) as search_attrib_lv,
	count(case when search_attributed_cart_add = 1 then visit_id end)/count(visit_id) as search_attrib_cart_add,
	count(case when search_attributed_purchase = 1 then visit_id end)/count(visit_id) as search_attrib_cr,
	sum(search_attributed_gms/100)/count(visit_id) as gms_per_query,
	sum(case when total_listing_views > 0 then avg_click_price_row end)/sum(case when total_listing_views > 0 then listing_count end) as avg_view_price_location,
	sum(case when total_cart_adds > 0 then avg_click_price_row end)/sum(case when total_cart_adds > 0 then listing_count end) as avg_atc_price_location,
	sum(case when total_purchases > 0 then avg_click_price_row end)/sum(case when total_purchases > 0 then listing_count end) as avg_purchase_price_location
	-- count(case when query_group_label = "Direct" then visit_id end)/count(visit_id) as direct_share
from
	`etsy-data-warehouse-dev.pdavidoff.aov_query_intent_groups`
where
	query_group_label is not null
group by 1
order by 5
;

select
	aov_supply_variability_group,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	count(*) as total_query_sessions,
	count(case when search_attributed_click = 1 then visit_id end)/count(visit_id) as search_attrib_lv,
	count(case when search_attributed_cart_add = 1 then visit_id end)/count(visit_id) as search_attrib_cart_add,
	count(case when search_attributed_purchase = 1 then visit_id end)/count(visit_id) as search_attrib_cr,
	sum(case when total_listing_views > 0 then avg_click_price_row end)/sum(case when total_listing_views > 0 then listing_count end) as avg_view_price_location,
	sum(case when total_cart_adds > 0 then avg_click_price_row end)/sum(case when total_cart_adds > 0 then listing_count end) as avg_atc_price_location,
	sum(case when total_purchases > 0 then avg_click_price_row end)/sum(case when total_purchases > 0 then listing_count end) as avg_purchase_price_location,
	-- avg(case when total_listing_views > 0 then avg_click_price_usd end) as avg_view_price,
	-- avg(case when total_cart_adds > 0 then avg_click_price_usd end) as avg_atc_price,
	-- avg(case when total_purchases > 0 then avg_click_price_usd end) as avg_purchase_price,
	sum(search_attributed_gms/100)/count(visit_id) as gms_per_query,
	count(case when query_group_label = "Direct" then visit_id end)/count(visit_id) as direct_share
from
	`etsy-data-warehouse-dev.pdavidoff.aov_query_intent_groups`
-- where
-- 	query_group_label is not null
group by 1
order by 2
;

-- look at summary stats for the median groups for direct queries only
select
	aov_supply_price_group,
	-- aov_supply_variability_group,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	count(*) as total_query_sessions,
	count(case when search_attributed_click = 1 then visit_id end)/count(visit_id) as search_attrib_lv,
	count(case when search_attributed_cart_add = 1 then visit_id end)/count(visit_id) as search_attrib_cart_add,
	count(case when search_attributed_purchase = 1 then visit_id end)/count(visit_id) as search_attrib_cr,
	sum(search_attributed_gms/100)/count(visit_id) as gms_per_query,
	sum(case when total_listing_views > 0 then avg_click_price_row end)/sum(case when total_listing_views > 0 then listing_count end) as avg_view_price_location,
	sum(case when total_cart_adds > 0 then avg_click_price_row end)/sum(case when total_cart_adds > 0 then listing_count end) as avg_atc_price_location,
	sum(case when total_purchases > 0 then avg_click_price_row end)/sum(case when total_purchases > 0 then listing_count end) as avg_purchase_price_location,
	count(case when total_purchases > 0 or next_visit_aov is not null then visit_id end)/count(visit_id) as overall_purchase_rate,
	avg(case when next_visit_aov is not null then next_visit_aov end) as next_purchase_aov,
	avg(case when next_visit_aov is not null then next_purchase_aiv end) as next_purchase_aiv,
	avg(case when next_visit_aov is not null then date_diff(next_purchase_date,visit_date,day) end) as next_purchase_days
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
where
	query_group_label = "Direct"
group by 1
order by 2
;

-- look at long tail of purchases after the query. how likely is it that a
-- buyer makes a purchase later?
select
	aov_supply_price_group,
	count(*) as total_query_sessions,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	count(case when search_attributed_click = 1 then visit_id end)/count(visit_id) as search_attrib_lv,
	count(case when search_attributed_cart_add = 1 then visit_id end)/count(visit_id) as search_attrib_cart_add,
	count(case when search_attributed_purchase = 1 then visit_id end)/count(visit_id) as search_attrib_cr,
	sum(search_attributed_gms/100)/count(visit_id) as gms_per_query,
	avg(case when search_attributed_purchase = 0 and next_visit_aov is not null then next_visit_aov end) as next_purchase_aov,
	avg(case when search_attributed_purchase = 0 and next_visit_aov is not null then next_purchase_aiv end) as next_purchase_aiv,
	count(case when search_attributed_purchase = 0 and next_visit_aov is not null then visit_id end)/count(visit_id) as later_purchase,
	avg(case when next_visit_aov is not null then date_diff(next_purchase_date,visit_date,day) end) as next_purchase_days
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
where
	visit_date between "2021-11-28" and "2021-11-28" + 7 and query_group_label = "Direct"
group by 1
order by 4
;

select
	aov_supply_price_group,
	date_diff(next_purchase_date,visit_date,day) as purchase_days,
	count(*) as total_purchases
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
where
	visit_date between "2021-11-28" and "2021-11-28" + 7 and query_group_label = "Direct"
group by 1,2
order by 1,2
;
	


-- top queries in the "very high" median group 
-- curve created to identify the expected purchase rate for each query based on their price.
create or replace table
  `etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
  as (
with base as (
select
	query,
	query_group_label,
	query_top_category,
	aov_supply_price_group,
	aov_supply_variability_group,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,	
	count(*) as query_sessions
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
group by 1,2,3,4,5
),row_base as (
select
	*
from
	(select *,row_number() over(partition by query order by query_sessions desc) as row from base)
where
	row = 1
),final_calcs as (
select
	a.query,
	a.query_group_label,
	a.aov_supply_price_group,
	a.aov_supply_variability_group,
	a.query_top_category,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	log(approx_quantiles(imp_price_median,100)[OFFSET(50)]) as log_mp,
	count(visit_id) as query_sessions,
	count(case when search_attributed_click > 0 then visit_id end) as clicks,
	count(case when search_attributed_cart_add > 0 then visit_id end) as cart_adds,
	count(case when search_attributed_purchase > 0 then visit_id end) as purchases,
	sum(search_attributed_gms/100) as search_attributed_gms,
	-- sum(total_listing_views) as total_listing_views,
	count(case when search_attributed_click > 0 then visit_id end)/count(visit_id) as click_rate,
	count(case when search_attributed_cart_add > 0 then visit_id end)/count(visit_id) as cart_add_rate,
	count(case when search_attributed_purchase > 0 then visit_id end)/count(visit_id) as purchase_rate,
	sum(search_attributed_gms/100)/count(*) as gms_per_query,
	count(case when buyer_segment = "Habitual" then visit_id end)/count(visit_id) as habitual_rate,
	count(case when buyer_segment = "High Potential" then visit_id end)/count(visit_id) as high_potential_rate,
	count(case when buyer_segment = "Repeat" then visit_id end)/count(visit_id) as repeat_rate,
	count(case when buyer_segment = "Active" then visit_id end)/count(visit_id) as active_rate,
	count(case when buyer_segment = "Not Active" then visit_id end)/count(visit_id) as not_active_rate,
	count(case when buyer_segment = "New" then visit_id end)/count(visit_id) as new_rate,
	count(case when buyer_segment = "Signed Out" then visit_id end)/count(visit_id) as signed_out_rate,
	count(case when buyer_segment in ("Habitual","High Potential","Repeat") then visit_id end)/count(visit_id) as overall_repeat_rate,
	avg(top_seller_share) as top_seller_share,
	avg(star_seller_share) as star_seller_share,
	avg(personalizable_share) as personalizable_share,
	avg(digital_share) as digital_share,
	avg(avg_favorite_count) as avg_favorite_count,
	avg(avg_tag_count) as avg_tag_count,
	avg(avg_image_count) as avg_image_count,
	avg(accepts_return_share) as accepts_return_share,
	avg(bp_enabled_share) as avg_bp_enabled_share,
	avg(avg_past_year_listing_reviews) as avg_past_year_listing_reviews,
	avg(avg_past_year_shop_reviews) as avg_past_year_shop_reviews,
	avg(avg_listing_rating) as avg_listing_rating,
	avg(avg_shop_rating) as avg_shop_rating,
	avg(avg_hobby_score) as avg_hobby_score,
	avg(avg_giftiness_score) as avg_giftiness_score,
	avg(query_group_label_conf_score) as avg_query_group_label_score
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases` a join
	row_base b on a.query = b.query and a.query_group_label = b.query_group_label and a.aov_supply_price_group = b.aov_supply_price_group and a.aov_supply_variability_group = b.aov_supply_variability_group and a.query_top_category = b.query_top_category and b.row = 1
group by 1,2,3,4,5
order by 7 desc
),exp_purchase_rate_calc as (
select
	*,
	0.325 + (-0.327*log_mp) + (0.148*pow(log_mp,2)) + (-0.03*pow(log_mp,3)) + (0.00218*pow(log_mp,4)) as exp_purchase_rate_orig,
	0.1224039 + (-0.2039959*log_mp) + (0.1037035*pow(log_mp,2)) + (-0.0219032*pow(log_mp,3)) + (0.0015930*pow(log_mp,4)) + (0.2198007*overall_repeat_rate) as exp_purchase_rate_new
from
	final_calcs
)
select
	*,
	purchase_rate / exp_purchase_rate_orig - 1 as purchase_rate_perf_orig,
	purchase_rate / exp_purchase_rate_new - 1 as purchase_rate_perf_new
from
	exp_purchase_rate_calc
where
	query_group_label = "Direct"
order by query_sessions desc
limit 1000
)
;


-- query characteristics
select
	case when purchase_rate_perf_new > 0 then "Overperform" else "Underperform" end as performance_group,
	approx_quantiles(quartile_1_price,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(median_price,100)[OFFSET(50)] as median_price,
	approx_quantiles(quartile_3_price,100)[OFFSET(50)] as quartile_3_price,
	count(*) as queries,
	sum(query_sessions) as total_query_sessions,
	avg(click_rate) as avg_click_rate,
	avg(cart_add_rate) as avg_cart_add_rate,
	avg(purchase_rate) as avg_purchase_rate,
	avg(exp_purchase_rate_new) as avg_purchase_rate
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	aov_supply_price_group = "Very High"
group by 1
order by 2 desc
;

-- buyer characteristics
select
	case when purchase_rate_perf_new > 0 then "Overperform" else "Underperform" end as performance_group,
	avg(avg_query_group_label_score) as avg_direct_query_score,
	avg(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(query, " "))+1) AS avg_query_word_count,
	avg(habitual_rate) as avg_habitual_rate,
	avg(high_potential_rate) as avg_high_potential_rate,
	avg(repeat_rate) as avg_repeat_rate,
	avg(active_rate) as avg_active_rate,
	avg(not_active_rate) as avg_not_active_rate,
	avg(new_rate) as avg_new_rate,
	avg(signed_out_rate) as avg_signed_out_rate
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	aov_supply_price_group = "Very High"
group by 1
order by 1 desc
;


-- shop characteristics
select
	case when purchase_rate_perf_new > 0 then "Overperform" else "Underperform" end as performance_group,
	avg(top_seller_share) as avg_top_seller_share,
	avg(star_seller_share) as avg_star_seller_share,
	avg(avg_bp_enabled_share) as avg_bp_enabled_share,
	avg(avg_past_year_shop_reviews) as avg_past_year_shop_reviews,
	avg(avg_shop_rating) as avg_shop_rating,
	avg(accepts_return_share) as avg_accepts_return
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	aov_supply_price_group = "Very High"
group by 1
order by 1 desc
;

-- listing characteristics
select
	case when purchase_rate_perf_orig > 0 then "Overperform" else "Underperform" end as performance_group,
	avg(avg_past_year_listing_reviews) as avg_past_year_listing_reviews,
	avg(avg_listing_rating) as avg_listing_rating,
	avg(avg_favorite_count) as avg_favorite_count,
	avg(avg_tag_count) as avg_tag_count,
	avg(avg_image_count) as avg_image_count,
	avg(avg_hobby_score) as avg_hobby_score,
	avg(avg_giftiness_score) as avg_giftiness_score,
	avg(personalizable_share) as avg_personalizable,
	avg(digital_share) as avg_digital
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	aov_supply_price_group = "Very High"
group by 1
order by 1 desc
;

-- top 10 and bottom 10
select
	query,
	query_group_label,
	aov_supply_price_group,
	aov_supply_variability_group,
	query_top_category,
	query_sessions,
	quartile_1_price,
	median_price,
	quartile_3_price,
	click_rate,
	cart_add_rate,
	purchase_rate,
	exp_purchase_rate,
	gms_per_query,
	purchase_rate_perf,
	top_seller_share,
	star_seller_share,
	habitual_rate+high_potential_rate+repeat_rate as repeat_buyer_rate,
	personalizable_share,
	avg_giftiness_score,
	avg_query_group_label_score
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	aov_supply_price_group = "Very High"
order by purchase_rate_perf desc
;

-- where are there more over and underperformers, by category?
select
	query_top_category,
	count(distinct query) as query_count,
	sum(query_sessions) as visit_count,
	sum(case when purchase_rate_perf_orig < 0 then query_sessions end)/sum(query_sessions) as underperform_rate,
	sum(case when purchase_rate_perf_orig < 0 then clicks end)/sum(case when purchase_rate_perf_orig < 0 then query_sessions end) as up_click_rate,
	sum(case when purchase_rate_perf_orig < 0 then cart_adds end)/sum(case when purchase_rate_perf_orig < 0 then query_sessions end) as up_cart_add_rate,
	sum(case when purchase_rate_perf_orig < 0 then purchases end)/sum(case when purchase_rate_perf_orig < 0 then query_sessions end) as up_purchase_rate,
	sum(case when purchase_rate_perf_orig >= 0 then clicks end)/sum(case when purchase_rate_perf_orig >= 0 then query_sessions end) as op_click_rate,
	sum(case when purchase_rate_perf_orig >= 0 then cart_adds end)/sum(case when purchase_rate_perf_orig >= 0 then query_sessions end) as op_cart_add_rate,
	sum(case when purchase_rate_perf_orig >= 0 then purchases end)/sum(case when purchase_rate_perf_orig >= 0 then query_sessions end) as op_purchase_rate
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	aov_supply_price_group in ("Very High")
group by 1
order by 2 desc
;


-- seller inventory
-- distribution of imp, click, atc, purchase price vs. inventory
with listings as (
select
	inventory_pctile,
	count(*) as listing_count,
	approx_quantiles(price_usd,100)[OFFSET(50)] as median_listing_price
from
	(select listing_id,price_usd,ntile(10) over(order by price_usd) as inventory_pctile from `etsy-data-warehouse-prod.rollups.active_listing_basics`)
group by 1
),listing_views as (
select
	view_pctile,
	count(*) as listing_view_count,
	approx_quantiles(price_usd,100)[OFFSET(50)] as median_listing_view_price
from
	(select listing_id,price_usd,ntile(10) over(order by price_usd) as view_pctile from `etsy-data-warehouse-prod.analytics.listing_views` where _date = "2021-12-12" and price_usd is not null)
group by 1
),atc as (
select
	atc_pctile,
	count(*) as atc_count,
	approx_quantiles(price_usd,100)[OFFSET(50)] as median_atc_price
from
	(select listing_id,price_usd,ntile(10) over(order by price_usd) as atc_pctile from `etsy-data-warehouse-prod.analytics.listing_views` where _date = "2021-12-12" and added_to_cart = 1 and price_usd is not null)
group by 1
),purchases as (
select
	a.receipt_id,
	count(distinct transaction_id) as trans_count,
	sum(quantity) as total_quantity,
	max(gms_net) as receipt_gms
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` a join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` b on a.receipt_id = b.receipt_id
where
	extract(date from a.creation_tsz) = "2021-12-12"
group by 1
having count(distinct transaction_id) = 1 and sum(quantity) = 1
),purchase_ntile as (
select
	trans_pctile,
	count(*) as receipt_count,
	approx_quantiles(receipt_gms,100)[OFFSET(50)] as median_aiv
from
	(select *,ntile(10) over(order by receipt_gms) as trans_pctile from purchases)
group by 1
)
select
	a.inventory_pctile as pctile,
	a.median_listing_price,
	b.median_listing_view_price,
	c.median_atc_price,
	d.median_aiv	
from
	listings a join
	listing_views b on a.inventory_pctile = b.view_pctile join
	atc c on a.inventory_pctile = c.atc_pctile join
	purchase_ntile d on a.inventory_pctile = d.trans_pctile
order by 1
;

with base as (
select
query,
query_top_category,
buyer_segment,
imp_price_median,
case 
  when buyer_segment in ("Habitual") then "Habitual"
  when buyer_segment in ("Repeat","High Potential") then "Repeat"
  when buyer_segment in ("Active") then "Active"
  when buyer_segment in ("Not Active","New") then "Not Active"
  when buyer_segment = "Signed Out" then buyer_segment
end as buyer_segment_group,
coalesce(search_attributed_click,0) as search_attributed_click,
coalesce(search_attributed_cart_add,0) as search_attributed_cart_add,
coalesce(search_attributed_purchase,0) as search_attributed_purchase,
round(imp_price_median) as round_price,
case 
  when imp_price_median <= 5 then 5
  when imp_price_median <= 10 then 10
  when imp_price_median <= 15 then 15
  when imp_price_median <= 20 then 20
  when imp_price_median <= 25 then 25
  when imp_price_median <= 30 then 30
  when imp_price_median <= 35 then 35
  when imp_price_median <= 40 then 40
  when imp_price_median <= 45 then 45
  when imp_price_median <= 50 then 50
  when imp_price_median <= 55 then 55
  when imp_price_median <= 60 then 60
  when imp_price_median <= 65 then 65
  when imp_price_median <= 70 then 70
  when imp_price_median <= 75 then 75
  when imp_price_median <= 80 then 80
  when imp_price_median <= 85 then 85
  when imp_price_median <= 90 then 90
  when imp_price_median <= 95 then 95
  when imp_price_median <= 100 then 100
  when imp_price_median > 100 then 101
end as median_imp_price_group_5,
    case 
  when imp_price_median <= 10 then 10
  when imp_price_median <= 20 then 20
  when imp_price_median <= 30 then 30
  when imp_price_median <= 40 then 40
  when imp_price_median <= 50 then 50
  when imp_price_median <= 60 then 60
  when imp_price_median <= 70 then 70
  when imp_price_median <= 80 then 80
  when imp_price_median <= 90 then 90
  when imp_price_median <= 100 then 100
  when imp_price_median > 100 then 101
end as median_imp_price_group_10

from
`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
where
query_group_label = "Direct"
order by rand()
)
select
	median_imp_price_group_5,
	buyer_segment_group,
	count(*) as query_sessions,
	sum(search_attributed_click) as total_clicks,
	sum(search_attributed_cart_add) as total_cart_adds,
	sum(search_attributed_purchase) as total_purchases,
	avg(search_attributed_click) as avg_click_rate,
	avg(search_attributed_cart_add) as avg_cart_add_rate,
	avg(purchase_rate) as avg_purchase_rate,
from
	base
group by 1,2
order by 1,2
;

-- let's learn about seller inventory
-- do sellers have price diversity? where does the median price fall for different seller tiers?
with base as (
select
	a.shop_id,
	a.listing_id,
	a.taxonomy_id,
	a.top_category,
	a.price_usd,
	b.seller_tier,
	a.is_digital,
	b.active_listings,
	b.last_star_seller_tier,
	b.buyer_promise_enabled,
	stddev(price_usd) over(partition by a.shop_id) as price_stddev
from
	`etsy-data-warehouse-prod.rollups.active_listing_basics` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.user_id = b.user_id
)
select
	seller_tier,
	count(distinct listing_id) as listing_count,
	approx_quantiles(price_usd,100)[OFFSET(25)] as q1_listing_price,
	approx_quantiles(price_usd,100)[OFFSET(50)] as med_listing_price,
	approx_quantiles(price_usd,100)[OFFSET(75)] as q3_listing_price
from
	base
group by 1
order by 2 desc
;



with base as (
select
	visit_id,
	query,
	max(price_row) as impressions,
	max(click) as query_level_click
from
	`etsy-data-warehouse-dev.pdavidoff.search_click_dist`
group by 1,2
)
select
	a.*,
	b.impressions as total_impressions
from
	`etsy-data-warehouse-dev.pdavidoff.search_click_dist` a join
	base b on a.visit_id = b.visit_id and a.query = b.query
where
	query_level_click = 1 and click = 1
order by visit_id,query
;


-- ACVV inventory shop analysis
select
	case when c.name in ("United States","Germany","United Kingdom","Australia",
	"France") then c.name else "ROW" end as country_name,
	count(distinct listing_id) as listing_count,
	count(distinct case when price_usd >= 50 then listing_id end)/count(distinct listing_id) as above_50_share
from
	`etsy-data-warehouse-prod.rollups.active_listing_basics` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.user_id = b.user_id join
	`etsy-data-warehouse-prod.etsy_v2.countries` c on b.country_id = c.country_id
where
	seller_tier in ("top seller","power seller")
group by 1
order by 3 desc
;



-- what is the opportunity around increasing conversion rates for underperforming queries?
-- first, we need to size the amount of GMS in the top 1000 queries. we'll extrapolate that number
-- to the others
-- overall, there is a large opportunity to increase purchase rate by 70%.
with base as (
select
	query,
	query_top_category,
	query_sessions,
	gms_per_query,
	purchases,
	purchase_rate,
	exp_purchase_rate_new,
	search_attributed_gms,
	purchase_rate_perf_new,
	search_attributed_gms/purchases as search_attr_aov,
	case when purchase_rate_perf_new >= 0 then "Overperform" else "Underperform" end as perform_group
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	purchases > 0
order by query_sessions desc
-- limit 1000
)
select
	avg(purchase_rate) as current_purchase_rate,
	avg(exp_purchase_rate_new) as exp_purchase_rate_new,
	sum((purchase_rate*query_sessions*search_attr_aov)) as current_attributed_gms,
	sum((exp_purchase_rate_new*query_sessions*search_attr_aov)-(purchase_rate*query_sessions*search_attr_aov)) as incremental_gms
from
	base
where
	perform_group = "Underperform"
;

select
	sum(case when median_price >= 45)

-- look at the overall opportunity for queries
with base as (
select
	query,
	query_top_category,
	aov_supply_price_group,
	query_group_label,
	query_sessions,
	gms_per_query,
	purchases,
	purchase_rate,
	exp_purchase_rate_new,
	case 
		when median_price > 100 then 0.008 
		else exp_purchase_rate_orig
	end as exp_purchase_rate_orig,	search_attributed_gms,
	purchase_rate_perf_new,
	search_attributed_gms/purchases as search_attr_aov,
	case when purchase_rate_perf_new >= 0 then "Overperform" else "Underperform" end as perform_group
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	purchases > 0 and median_price >= 45
order by query_sessions desc
-- limit 1000
)
select
	sum(query_sessions) as total_query_sessions,
	sum(purchase_rate*query_sessions*search_attr_aov) as current_attributed_gms,
	avg(purchase_rate) as current_purchase_rate,
	sum(case when perform_group = "Underperform" then query_sessions end) as up_query_sessions,
	sum(case when perform_group = "Underperform" then (purchase_rate*query_sessions*search_attr_aov) end) as up_current_attributed_gms,
	sum(case when perform_group = "Underperform" then search_attributed_gms end)/sum(case when perform_group = "Underperform" then purchases end) as up_aov,
	avg(case when perform_group = "Underperform" then purchase_rate end) as up_current_purchase_rate,
	avg(case when perform_group = "Underperform" then exp_purchase_rate_orig end) as up_exp_purchase_rate_new,
	sum(case when perform_group = "Underperform" then (exp_purchase_rate_orig*query_sessions*search_attr_aov)-(purchase_rate*query_sessions*search_attr_aov) end) as up_incremental_gms
from
	base
;

with base as (
select
	query,
	query_top_category,
	aov_supply_price_group,
	query_group_label,
	query_sessions,
	gms_per_query,
	purchases,
	purchase_rate,
	exp_purchase_rate_new,
	median_price,
	case 
		when median_price > 100 then 0.008 
		else exp_purchase_rate_orig
	end as exp_purchase_rate_orig,	search_attributed_gms,
	purchase_rate_perf_new,
	search_attributed_gms/purchases as search_attr_aov,
	case when purchase_rate_perf_new >= 0 then "Overperform" else "Underperform" end as perform_group
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	purchases > 0
order by query_sessions desc
-- limit 1000
)
select
	sum(case when median_price > 25 then (purchase_rate*query_sessions*search_attr_aov) end)/sum(purchase_rate*query_sessions*search_attr_aov) as over_25_share,
	sum(case when median_price >= 45 then (purchase_rate*query_sessions*search_attr_aov) end)/sum(purchase_rate*query_sessions*search_attr_aov) as over_45_share

from
	base
where
	purchases > 0
;


-- what is the gms coverage for search? what is the gms coverage for direct queries?
select
	sum(attributed_gms/100) as search_attributed_gms
from
	`etsy-data-warehouse-prod.search.query_sessions_new`
where
	_date between "2021-11-28" and "2021-12-27"
;

select
	sum(total_gms)
from
	`etsy-data-warehouse-prod.weblog.visits`
where
	_date between "2021-11-28" and "2021-12-27"
;

select
	query_group_label,
	sum(search_attributed_gms)/sum(sum(search_attributed_gms)) over() as search_attributed_gms
from
	`etsy-data-warehouse-dev.pdavidoff.aov_query_intent_groups`
group by 1
;

with base as (
select
	query,
	query_top_category,
	aov_supply_price_group,
	query_group_label,
	case
		when median_price between 0 and 10 then 10
		when median_price between 10 and 20 then 20
		when median_price between 20 and 30 then 30
		when median_price between 30 and 40 then 40
		when median_price between 40 and 50 then 50
		when median_price between 50 and 60 then 60
		when median_price between 60 and 70 then 70
		when median_price between 70 and 80 then 80
		when median_price between 80 and 90 then 90
		when median_price between 90 and 100 then 100
		when median_price > 100 then 101
		else null
	end as price_group,
	query_sessions,
	gms_per_query,
	purchases,
	purchase_rate,
	exp_purchase_rate_new,
	case 
		when median_price > 100 then 0.008 
		else exp_purchase_rate_orig
	end as exp_purchase_rate_orig,
	search_attributed_gms,
	purchase_rate_perf_new,
	search_attributed_gms/purchases as search_attr_aov,
	case when purchase_rate_perf_new >= 0 then "Overperform" else "Underperform" end as perform_group
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	purchases > 0
order by query_sessions desc
-- limit 1000
)
select
	price_group,
	sum(query_sessions) as total_query_sessions,
	sum(query_sessions)/sum(sum(query_sessions)) over() as query_session_share,
	sum(purchase_rate*query_sessions*search_attr_aov) as current_attributed_gms,
	sum(purchase_rate*query_sessions*search_attr_aov)/sum(sum(purchase_rate*query_sessions*search_attr_aov)) over() as attributed_gms_share,
	sum(search_attributed_gms)/sum(purchases) as overall_search_aov,
	avg(purchase_rate) as current_purchase_rate,
	sum(case when perform_group = "Underperform" then query_sessions end) as up_query_sessions,
	sum(case when perform_group = "Underperform" then (purchase_rate*query_sessions*search_attr_aov) end) as up_current_attributed_gms,
	sum(case when perform_group = "Underperform" then search_attributed_gms end)/sum(case when perform_group = "Underperform" then purchases end) as up_aov,
	avg(case when perform_group = "Underperform" then purchase_rate end) as up_current_purchase_rate,
	avg(case when perform_group = "Underperform" then exp_purchase_rate_orig end) as up_exp_purchase_rate_new,
	sum(case when perform_group = "Underperform" then (exp_purchase_rate_orig*query_sessions*search_attr_aov)-(purchase_rate*query_sessions*search_attr_aov) end) as up_incremental_gms
from
	base
group by 1
order by 1
;
	
-- look at the data by price group. pretty consistent, which isn't surprising because
-- the curve is based on price
with base as (
select
	query,
	query_top_category,
	aov_supply_price_group,
	query_group_label,
	query_sessions,
	gms_per_query,
	purchases,
	purchase_rate,
	exp_purchase_rate_new,
	case 
		when median_price > 100 then 0.008 
		else exp_purchase_rate_orig
	end as exp_purchase_rate_orig,
	search_attributed_gms,
	purchase_rate_perf_new,
	search_attributed_gms/purchases as search_attr_aov,
	case when purchase_rate_perf_new >= 0 then "Overperform" else "Underperform" end as perform_group
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	purchases > 0
order by query_sessions desc
-- limit 1000
)
select
	aov_supply_price_group,
	sum(query_sessions) as total_query_sessions,
	sum(query_sessions)/sum(sum(query_sessions)) over() as query_session_share,
	sum(purchase_rate*query_sessions*search_attr_aov) as current_attributed_gms,
	sum(purchase_rate*query_sessions*search_attr_aov)/sum(sum(purchase_rate*query_sessions*search_attr_aov)) over() as attributed_gms_share,
	sum(search_attributed_gms)/sum(purchases) as overall_search_aov,
	avg(purchase_rate) as current_purchase_rate,
	sum(case when perform_group = "Underperform" then query_sessions end) as up_query_sessions,
	sum(case when perform_group = "Underperform" then (purchase_rate*query_sessions*search_attr_aov) end) as up_current_attributed_gms,
	sum(case when perform_group = "Underperform" then search_attributed_gms end)/sum(case when perform_group = "Underperform" then purchases end) as up_aov,
	avg(case when perform_group = "Underperform" then purchase_rate end) as up_current_purchase_rate,
	avg(case when perform_group = "Underperform" then exp_purchase_rate_orig end) as up_exp_purchase_rate_new,
	sum(case when perform_group = "Underperform" then (exp_purchase_rate_orig*query_sessions*search_attr_aov)-(purchase_rate*query_sessions*search_attr_aov) end) as up_incremental_gms
from
	base
where
	perform_group = "Underperform"
group by 1
order by 5 desc
;

-- overall opportunity
	

-- category view
with base as (
select
	query,
	query_top_category,
	aov_supply_price_group,
	query_group_label,
	query_sessions,
	gms_per_query,
	purchases,
	purchase_rate,
	exp_purchase_rate_new,
	case 
		when median_price > 100 then 0.008 
		else exp_purchase_rate_orig
	end as exp_purchase_rate_orig,
	search_attributed_gms,
	purchase_rate_perf_new,
	search_attributed_gms/purchases as search_attr_aov,
	case when purchase_rate_perf_new >= 0 then "Overperform" else "Underperform" end as perform_group
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
where
	purchases > 0 and median_price >=
order by query_sessions desc
-- limit 1000
)
select
	query_top_category,
	sum(query_sessions) as total_query_sessions,
	sum(query_sessions)/sum(sum(query_sessions)) over() as query_session_share,
	sum(purchase_rate*query_sessions*search_attr_aov) as current_attributed_gms,
	sum(purchase_rate*query_sessions*search_attr_aov)/sum(sum(purchase_rate*query_sessions*search_attr_aov)) over() as attributed_gms_share,
	sum(search_attributed_gms)/sum(purchases) as overall_search_aov,
	avg(purchase_rate) as current_purchase_rate,
	sum(case when perform_group = "Underperform" then query_sessions end) as up_query_sessions,
	sum(case when perform_group = "Underperform" then (purchase_rate*query_sessions*search_attr_aov) end) as up_current_attributed_gms,
	sum(case when perform_group = "Underperform" then search_attributed_gms end)/sum(case when perform_group = "Underperform" then purchases end) as up_aov,
	avg(case when perform_group = "Underperform" then purchase_rate end) as up_current_purchase_rate,
	avg(case when perform_group = "Underperform" then exp_purchase_rate_orig end) as up_exp_purchase_rate_new,
	sum(case when perform_group = "Underperform" then (exp_purchase_rate_orig*query_sessions*search_attr_aov)-(purchase_rate*query_sessions*search_attr_aov) end) as up_incremental_gms
from
	base
group by 1
order by 2 desc
;
-- 66% GMS coverage for underperformers








-------------------------------------APPENDIX-----------------------------------------------
-------------------------------------APPENDIX-----------------------------------------------
-------------------------------------APPENDIX-----------------------------------------------
-------------------------------------APPENDIX-----------------------------------------------
-------------------------------------APPENDIX-----------------------------------------------
-------------------------------------APPENDIX-----------------------------------------------



create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.module_impressions`
	as (
select
	distinct
	_date as date,
	a.visit_id,
	timestamp_millis(b.epoch_ms) as event_time,
	(select value from unnest(properties.map) where key = "module_placement") as query,
	(select value from unnest(properties.map) where key = "listing_id") as prolist_listings,
	(select value from unnest(properties.map) where key = "predCtr") as pred_ctr,
	(select value from unnest(properties.map) where key = "predCvr") as pred_cvr,
	(select value from unnest(properties.map) where key = "page_type") as page_type,
	(select value from unnest(properties.map) where key = "guid") as guid,
	(select value from unnest(properties.map) where key = "page_guid") as page_guid,
	(select value from unnest(properties.map) where key = "ref") as ref,
	(select value from unnest(properties.map) where key = "loc") as loc
from
	`etsy-visit-pipe-prod.canonical.visits` a join
	UNNEST(a.events.events_tuple) AS b on b.event_type = "recommendations_module_delivered"
where
	_date BETWEEN "2021-10-19" and "2021-10-25"
)
;
-- search impressions and purchase price
with visit_purchases as (
select
	distinct
	visit_id,
	query
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions`
where
	purchases > 0 and _date = current_date - 2
),base as (
select
	a.visit_id,
	a.query,
	a.page,
	a.listing_id,
	clicks,
	purchases,
	price_usd,
	impressions
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions` a join
	`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = b.listing_id join
	visit_purchases c on a.visit_id = c.visit_id and a.query = c.query
where
	_date = current_date - 2
)
select
	visit_id,
	query,
	sum(price_usd)/sum(impressions) as price_per_impression,
	sum(case when clicks > 0 then price_usd*clicks end)/sum(clicks) as price_per_click,
	sum(case when purchases > 0 then price_usd*purchases end)/sum(purchases) as price_per_purchase
from
	base
group by 1,2
order by 1,2
limit 50;

-- let's look at shops. do our sellers generally have high or low price diversity?
-- how does that differ based on their shop's median price?


with base as (
select
	a.shop_id,
	a.seller_tier,
	b.listing_id,
	price_usd
from
	`etsy-data-warehouse-prod.rollups.seller_basics` a join
	`etsy-data-warehouse-prod.rollups.active_listing_basics` b on a.shop_id = b.shop_id
),base2 as (
select
	shop_id,
	ntile(10) over(partition by shop_id order by price_usd) as price_pctile,
	price_usd,
	listing_id
from
	base
)




create or replace table 
	`etsy-data-warehouse-dev.pdavidoff.search_click_dist`
	as (
with clicks as (
select
	a.visit_id,
	a.listing_id,
	case 
		when substr(a.ref_tag,1,3) = "sr_" then "Organic"
		when substr(a.ref_tag,1,3) = "sc_" then "Paid"
		else "Other"
	end as listing_type,
	case
		when substr(a.ref_tag,13,1) = "-" then substr(a.ref_tag,12,1) 
		else substr(a.ref_tag,12,2) 
	end as page_no,
	case
		when substr(a.ref_tag,15,1) = "" then substr(a.ref_tag,14,1)
		else substr(a.ref_tag,14,2)
	end as position,
	a.sequence_number,
	a.ref_tag,
	case 
		when strpos(url,"&ref=")-strpos(url,"ga_search_query=")-16 > 0 then 
			replace(substr(url,strpos(url,"ga_search_query=")+16,strpos(url,"&ref=")-strpos(url,"ga_search_query=")-16),"+"," ")
		else "" 
	end as query,
	strpos(url,"&ref=") as ref_pos,
	strpos(url,"ga_search_query=") as ga_search_q_pos,
	b.url
from
	`etsy-data-warehouse-prod.analytics.listing_views` a join
	`etsy-data-warehouse-prod.weblog.events` b on a.sequence_number = b.sequence_number and a.visit_id = b.visit_id and b._date = "2021-12-18"
where
	a.platform in ("desktop","mobile_web") and a.referring_page_event = "search"
	and a._date = "2021-12-18" and a.ref_tag like "%gallery%"
),imp_base as (
	select
		distinct
		a.visit_id,
		a.query,
		a.listing_id,
		b.price_usd/100 as price_usd,
		a.impressions,
		a.clicks,
		a.carts,
		a.purchases,
		a.page,
		c.taxonomy_id,
		c.path as taxo_leaf_node,
		row_number() over(partition by visit_id,query order by price_usd) as price_row,
		percentile_cont(price_usd,0.25) over(partition by visit_id,query) as imp_price_quartile_1,
		percentile_cont(price_usd,0.5) over(partition by visit_id,query) as imp_price_median,
		percentile_cont(price_usd,0.75) over(partition by visit_id,query) as imp_price_quartile_3
	from
		`etsy-data-warehouse-prod.search.visit_level_listing_impressions` a join
		`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = b.listing_id join
		`etsy-data-warehouse-prod.materialized.listing_taxonomy` c on b.listing_id = c.listing_id
	where
		_date = "2021-12-18" and page = "search"
)
select
	a.visit_id,
	a.query,
	a.listing_id,
	a.price_usd,
	a.taxonomy_id,
	taxo_leaf_node,
	price_row,
	case when b.listing_id is not null then 1 else 0 end as click,
	listing_type,
	page_no,
	position,
	imp_price_quartile_1,
	imp_price_median,
	imp_price_quartile_3
from
	imp_base a left join
	clicks b on a.visit_id = b.visit_id and a.query = b.query and a.listing_id = b.listing_id
)
;


select
	*
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary`
where
	date = "2022-01-05"
;
