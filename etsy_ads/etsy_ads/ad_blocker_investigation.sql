-- there was an ad blocker change on 11/14. investigate the impact of the change on revenue
with gms as (
select
	date,
	sum(accounting_gms_net) as gms_net	
from
	`etsy-data-warehouse-prod.rollups.gms_daily_mart_regional_yy`
where
	date >= "2017-01-01"
group by 1
),spend as (
select
	date,
	sum(spend) as total_spend
from
	`etsy-data-warehouse-prod.rollups.prolist_daily_summary`
where
	date >= "2017-01-01"
group by 1
),base3 as (
select
	a.date,
	a.gms_net,
	b.total_spend	
from
	gms a join
	spend b on a.date = b.date
)
select
	a.*,
	a.gms_net/b.gms_net - 1 as gms_yy,
	a.total_spend/b.total_spend - 1 as spend_yy
from
	base3 a left join
	base3 b on a.date = date_add(b.date, interval 52 WEEK) 
limit 50
;
select
	distinct
	prolist_platform
from
	`etsy-prolist-etl-prod.prolist.attributed_impressions`
where
	_PARTITIONTIME >= "2021-11-18"
;

select
		
from
	`etsy-data-warehouse-prod.rollups.prolist_click_mart`

with base as (
select
	platform,
	date(timestamp(cast(click_timestamp as datetime),"UTC"),"America / New York") as date,
	sum(cost) as total_spend,
	count(*) as total_clicks,
	sum(cost)/count(*) as cpc
from
	`etsy-data-warehouse-prod.rollups.prolist_click_mart`
where
	date(timestamp(cast(click_timestamp as datetime),"UTC"),"America / New York") >= "2020-09-15"
group by 1,2
),gms as (
select
	platform,
	_date as date,
	count(visit_id) as visit_count,
	count(case when converted = 1 then visit_id end)/count(visit_id) as visit_cr,
	avg(case when converted = 1 then total_gms end) as acvv
from
	`etsy-data-warehouse-prod.weblog.visits`
where
	_date >= "2020-09-15"
group by 1
)
select
	a.date,
	a.platform,
	a.total_spend,
	a.total_clicks,
	a.cpc,
	c.visit_count,
	c.visit_cr,
	c.acvv,
	a.total_spend/a.total_gms as take_rate,
	-- a.total_imps,
	a.total_spend/b.total_spend -1 as spend_yy,
	a.total_clicks/b.total_clicks - 1 as clicks_yy,
	a.cpc/b.cpc - 1 as cpc_yy,
	a.visit_count/b.visit_count - 1 as visit_yy,
	a.visit_cr/b.visit_cr - 1 as visit_cr_yy,
	a.acvv/b.acvv - 1 as acvv_yy,
	(a.total_spend/c.total_gms)/(b.total_spend/d.total_gms)-1 as take_rate_yy
	-- a.total_imps/b.total_imps - 1 as imps_yy
from
	base a join
	base b on a.date = date_add(b.date, interval 52 week) and a.platform = b.platform join
	gms c on a.date = c.date and a.platform = c.platform join
	gms d on a.date = date_add(b.date, interval 52 week) and a.platform = b.platform
where
	a.date >= "2021-10-01"
order by 1,2
;
