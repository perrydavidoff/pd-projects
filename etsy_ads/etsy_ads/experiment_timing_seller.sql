with base as (
	select
		b.user_id,
		budget_constrained_shop,
		max(budget) as max_budget,
		min(budget) as min_budget
	from
		`etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` a join
		`etsy-data-warehouse-prod.rollups.seller_basics` b on a.shop_id = b.shop_id
	where
		current_date - 30 <= date
	group by 1,2
)
select
	-- _date,
	count(distinct case when event_type = "mc_seller_dashboard" and b.user_id is not null then a.user_id end) as dashboard_users,
	count(distinct case when event_type = "mcnav_secondary_etsy-ads" then a.user_id end) as ea_dashboard_users,
	count(distinct case when event_type = "mc_seller_dashboard" and min_budget != max_budget then a.user_id end) as dashboard_users_budget_chg,
	count(distinct case when event_type = "mcnav_secondary_etsy-ads" and min_budget != max_budget then a.user_id end) as ea_dashboard_users_budget_chg,
	count(distinct case when event_type = "mc_seller_dashboard" and budget_constrained_shop = 1 then a.user_id end) as dashboard_users,
	count(distinct case when event_type = "mcnav_secondary_etsy-ads" and budget_constrained_shop = 1 then a.user_id end) as ea_dashboard_users,
	count(distinct case when event_type = "mc_seller_dashboard" and budget_constrained_shop = 1 and min_budget != max_budget then a.user_id end) as bc_dashboard_users_budget_chg,
	count(distinct case when event_type = "mcnav_secondary_etsy-ads" and budget_constrained_shop = 1 and min_budget != max_budget then a.user_id end) as bc_ea_dashboard_users_budget_chg
from
	`etsy-data-warehouse-prod.weblog.events` a left join
	base b on a.user_id = b.user_id
where
	event_type in ("mc_seller_dashboard","mcnav_secondary_etsy-ads")
;
	
