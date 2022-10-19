    select 
        date_trunc(_date,week) as week,
        count(distinct visit_id) as visit_count,
        count(distinct browser_id) as browser_count,
        count(distinct case when user_id is null then visit_id end)/count(distinct visit_id) as visit_share,
        count(distinct case when user_id is null then browser_id end)/count(distinct browser_id) as browser_share
    from 
        `etsy-data-warehouse-prod.weblog.visits`
    where
        _date >= "2020-10-01"
    group by 1
    order by 1
;