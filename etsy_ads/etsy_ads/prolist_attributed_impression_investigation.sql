-- impression check
with impr_base as (
    select 
        a._date as date,
        count(*) as prolist_imps_events,
        count(case when platform = "desktop" then a.run_date end) as desktop_imp,
        count(case when platform = "mobile_web" then a.run_date end) as mweb_imp,
        count(case when platform = "boe" then a.run_date end) as boe_imp
    from
        `etsy-data-warehouse-prod.weblog.events` a join
        `etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date >= "2022-01-01"
    where
        event_type = "prolist_imp_full" and a._date >= current_date - 30
    group by 1
),attr_impr_table as (
    select 
        _PARTITIONDATE as date,
        count(*) as table_impr_count,
        count(case when prolist_platform in ("desktop_web") then uuid end) as desktop_imps,
        count(case when prolist_platform in ("mobile_web") then uuid end) as mweb_imps,
        count(case when prolist_platform in ("android","ios") then uuid end) as boe_imps
    from
        `etsy-prolist-etl-prod.prolist.attributed_impressions`
    where
        _PARTITIONDATE >= current_date - 30
    group by 1
)
select 
    a.date,
    a.prolist_imps_events,
    b.table_impr_count,
    b.table_impr_count/a.prolist_imps_events as total_coverage,
    b.desktop_imps/a.desktop_imp as desktop_coverage,
    b.mweb_imps/a.mweb_imp as mweb_coverage,
    b.boe_imps/a.boe_imp as boe_coverage
from 
    impr_base a left join
    attr_impr_table b on a.date = b.date
order by 1
;

-- click check
with click_event_table as (
    select 
        a._date as date,
        count(*) as prolist_click_event
    from
        `etsy-data-warehouse-prod.weblog.events` a join
        `etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id and b._date >= current_date - 30
    where
        event_type = "prolist_click_full" and a._date >= current_date - 30
    group by 1
),prolist_click_log as (
select
    date(timestamp_seconds(click_date)) as date,
    count(*) as prolist_click_log_table
from
    `etsy-data-warehouse-prod.etsy_shard.prolist_click_log`
where
    date(timestamp_seconds(click_date)) >= current_date - 30
group by 1
),attrib_imps as (
select
    _PARTITIONDATE as date,
    count(*) as attrib_imp_clicks
from
    `etsy-prolist-etl-prod.prolist.attributed_impressions`
where
    _PARTITIONDATE >= current_date - 30 and click = 1
group by 1
)
select
    a.date,
    a.prolist_click_event,
    b.prolist_click_log_table,
    c.attrib_imp_clicks
from
    click_event_table a left join
    prolist_click_log b on a.date = b.date left join
    attrib_imps c on a.date = c.date
order by a.date
;

