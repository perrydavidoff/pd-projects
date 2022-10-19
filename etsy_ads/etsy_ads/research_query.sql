with can_be_contacted_feedback as (
select
	distinct
	a.user_id
from
	`etsy-data-warehouse-prod.etsy_index.users_index` a join
	`etsy-data-warehouse-prod.rollups.billing_users_unique` b on a.user_id = b.user_id join
	`etsy-data-warehouse-prod.rollups.email_subscribers` c on a.user_id = c.user_id
where
 a.is_seller = 1 and
 a.is_admin = 0 and
 a.is_nipsa = 0 and
 a.is_frozen = 0 and 
 b.country_id in (209) and
 a.user_state = "active" and
 c.campaign_label = "feedback" and
 c.campaign_state = "subscribed" and
 c.verification_state = "confirmed" and 
 a.user_id not in 
 	(select distinct user_id
 		from
 			`etsy-data-warehouse-prod.etsy_shard.user_preferences`
 		where
 			preference_id = 704 and preference_setting = "false")
)
select
	distinct
 a.user_id,
 c.primary_email as Email,
 c.shop_name,
 g.login_name,
 c.seller_tier,
 c.country_name,
 c.usa_county,
 c.usa_city || ", " || c.usa_state as location,
 c.usa_zip as zip_code,
 c.past_year_gms,
 c.past_year_orders,
 c.pattern_enabled,
 c.open_date,
 c.top_category_new,
 c.active_listings,
 c.gms_decile,
 c.total_gms,
 c.total_intl_gms,
 c.total_orders,
 c.has_labels_rev,
 c.has_ads_rev,
 c.teams_active,
 c.is_etsy_plus
 from
 can_be_contacted_feedback a join 
 `etsy-data-warehouse-prod.rollups.seller_basics` c on a.user_id = c.user_id join 
 `etsy-data-warehouse-prod.etsy_index.users_index` g on a.user_id = g.user_id join 
 `etsy-data-warehouse-prod.etsy_shard.shop_data` v on a.user_id = v.user_id join
 `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` pro on c.shop_id = pro.shop_id and date <= current_date - 60 and budget_constrained_shop = 1
 
-- WHAT KIND OF SELLERS DO YOU NEED?
where
  c.shop_status = "active"
-- ADD TOP SELLERS 
  AND c.top_seller_status = 1
-- DON"T TARGET NYC SELLERS FOR REMOTE RESEARCH W/O TALKING TO POLINA:
 -- AND NOT (c.usa_state = 'NY' AND c.usa_county in ('QUEENS', 'KINGS', 'RICHMOND', 'BRONX', 'NEW YORK'))
  AND v.is_vacation = 0
  AND c.past_year_gms > 500
 
  order by rand()
 
-- HOW MANY SELLER DO YOU NEED?
limit 500;
