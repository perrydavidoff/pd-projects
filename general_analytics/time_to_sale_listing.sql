with base as (
select
	listing_id,
	extract(date from (timestamp_seconds(original_create_date))) as original_create_date
from
	`etsy-data-warehouse-prod.listing_mart.listings`
where
	extract(year from timestamp_seconds(original_create_date)) = 2021
),base2 as (
select
	a.listing_id,
	original_create_date,
	date_add(original_create_date,interval 4 month) as expiry_date,
	min(case when b.type_id is not null then date else date_add(original_create_date,interval 4 month) end) as first_action_date
from
	base a left join
	`etsy-data-warehouse-prod.bill_mart_ledger.ledger_joined` b on a.listing_id = b.type_id and type in 
	("auto_renew_expired","auto_renew_expired_refund","listing","listing_private","listing_private_refund",
		"listing_refund","renew","renew_expired","renew_expired_refund","renew_refund","renew_sold","renew_sold_auto",
		"renew_sold_auto_refund","renew_sold_refund","transaction_quantity","transaction_quantity_refund") and b.date > original_create_date
group by 1,2,3
)
select
	case when first_action_date < expiry_date then 1 else 0 end as sale,
	count(distinct listing_id) as listing_count,
	avg(date_diff(first_action_date,original_create_date,day)) as avg_action_days
from
	base2
-- group by 1
;

with base as (
select
	listing_id,
	extract(date from (timestamp_seconds(original_create_date))) as original_create_date
from
	`etsy-data-warehouse-prod.listing_mart.listings`
where
	extract(year from timestamp_seconds(original_create_date)) = 2020
),base2 as (
select
	a.listing_id,
	original_create_date,
	date_add(original_create_date,interval 4 month) as expiry_date,
	min(case when b.listing_id is not null then extract(date from b.creation_tsz) else date_add(original_create_date,interval 4 month) end) as first_action_date
from
	base a left join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` b on a.listing_id = b.listing_id and extract(date from b.creation_tsz) >= original_create_date
group by 1,2,3
)
select
	case when first_action_date < expiry_date then 1 else 0 end as sale,
	count(distinct listing_id) as listing_count,
	avg(date_diff(first_action_date,original_create_date,day)) as avg_action_days
from
	base2
group by 1
;
