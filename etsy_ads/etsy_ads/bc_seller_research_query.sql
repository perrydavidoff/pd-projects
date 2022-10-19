-- This query is a template, please make a copy before editing.
-- To run you need to edit:
 -- Countries of interest
 -- Variables list
 -- WHERE statement to pick your seller type
 -- LIMIT statement to select size of sample
 
with 

 
with can_be_contacted_feedback as (
 select distinct ui.user_id from `etsy-data-warehouse-prod.etsy_index.users_index` ui
 where
   ui.is_seller = 1
   and ui.is_admin = 0
   and ui.is_nipsa = 0
   and ui.is_frozen = 0
   and ui.user_state = "active"
   and ui.user_id in (
     select user_id
     from `etsy-data-warehouse-prod.rollups.active_sellers_rollup_daily`
   )
   and ui.user_id not in (
     select distinct user_id
     from `etsy-data-warehouse-prod.research.ux_participants`
     where date(date_contacted) >= date_sub(current_date,interval 6 month)
   )
 -- WHICH COUNTRIES?  USA (209), Canada(79), UK (105)
   and ui.user_id in (
    select distinct user_id
    from `etsy-data-warehouse-prod.rollups.billing_users_unique`
    where country_id in (209)
   )
   and ui.user_id in (
     select distinct user_id
     from `etsy-data-warehouse-prod.rollups.email_subscribers`
     where
       campaign_label = "feedback"
       and campaign_state = "subscribed"
       and verification_state = "confirmed"
   )
   and ui.user_id not in (
     select distinct user_id
     from `etsy-data-warehouse-prod.etsy_shard.user_preferences`
     where
       preference_id = 704
       and preference_setting = "false"
   ) 
   -- ADD BC USERS WHO JOINED ADS AT LEAST 60 DAYS AGO
   --   and ui.user_id in (
   --   select
   --        shop_id
   --    from `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
   --    where budget_constrained_shop = 1
   --    group by 1
   --    having min(date) >= date_sub(current_date(), interval 60 day)
   -- )
 )  
 
-- WHAT DATA IS USEFUL?
select
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
 `etsy-data-warehouse-prod.etsy_shard.shop_data` v on a.user_id = v.user_id
 
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