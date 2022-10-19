select
	date_trunc(click_timestamp,week) as week,
	sum(case when a.platform_os in ("android","ios") and is_tablet = 0 then cost end) as mobile_phone_revenue,
	sum(case when a.platform_os in ("android","ios") and is_tablet = 1 then cost end) as tablet_rev,
	sum(case when a.platform_os = "desktop" then cost end) as desktop_rev,
	sum(case when a.platform_os not in ("desktop","android","ios") then cost end) as other_rev
from
	`etsy-data-warehouse-prod.rollups.prolist_click_mart` a join
	`etsy-data-warehouse-prod.weblog.visits` b on a.visit_id = b.visit_id and b._date >= "2018-01-01"
where
	click_timestamp >= "2018-01-01"
group by 1
order by 1
;

select
	distinct
	is_tablet,
	count(*)
from
	`etsy-data-warehouse-prod.weblog.recent_visits`
where
	_date >= "2022-03-01"
group by 1
order by 2 desc
;

select
	initiative,
	subteam,
	count(*)
from
	`etsy-data-warehouse-prod.catapult.catapult_experiment_reports_with_goals`
where
	end_date >= "2022-01-01"
group by 1,2
order by 3 desc
;

select
	distinct
	initiative,
	count(*)
from
	`etsy-data-warehouse-prod.catapult.catapult_experiment_reports_with_goals`
where
	end_date >= "2022-01-01"
group by 1
order by 2 desc
;