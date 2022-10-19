-- look into historical experiments launched by ads teams
select
	-- a.launch_date,
	start_date,
	end_date,
	extract(year from end_date) as end_year,
	a.initiative,
	a.subteam,
	a.experiment_name,
	-- a.start_date,
	-- a.end_date,
	a.gms_coverage,
	a.conv_pct_change as banked_cr_change,
	a.winsorized_acbv_pct_change as banked_acbv_change,
	a.gms_ann,
	a.rev_coverage,
	a.prolist_pct_change,
	-- a.gms_coverage,
	-- a.rev_ann,
	layer_start,
	layer_end,
	status
	-- a.launch_id
from
	`etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` a left join
	`etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b on a.experiment_name = b.name
where
	subteam in ("Buyer Ads Experience","Ad Ranking","Ad Delivery","Seller Ad Experience","Ads",
		"Ad Marketplace Dynamics","Ad Platform") and end_date between "2019-01-01" and "2022-08-18"
	-- and prolist_pct_change > 0 and 
	-- status like "%Ramped Up%"
order by end_date
;

select
	date_trunc(date,month) as month,
	sum(etsy_visits) as etsy_visits,
	sum(impressions) as total_impressions,
	sum(clicks) as total_clicks,
	sum(converting_clicks) as total_conv_clicks,
	sum(revenue) as total_ea_revenue,
	sum(ly_etsy_visits) as ly_etsy_visits,
	sum(ly_impressions) as ly_impressions,
	sum(ly_clicks) as ly_clicks,
	sum(ly_converting_clicks) as ly_conv_clicks,
	sum(ly_revenue) as ly_revenue
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary`
where
	date >= "2019-01-01"
group by 1
order by 1
;

