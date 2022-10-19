CREATE OR REPLACE TEMPORARY TABLE seller_churn
    AS select 
        a.date,
        a.seller_tier,
        a.budget_tier,
        count(distinct case when a.sum_of_budget_30_days  = 0 AND a.budget_31_days_ago >= 1 then a.shop_id end) AS num_of_seller_churn,
        count(distinct case when a.sum_of_budget_30_days  = 0 AND a.budget_31_days_ago >= 1 then a.shop_id end) / 
        nullif(avg(distinct case when a.sum_of_budget_30_days  > 0 AND a.budget_31_days_ago >= 1 then b.num_sellers_impressions_l4w end),0) AS seller_churn_rate
        -- active EA seller is defined as having an ads impression in the last 4 weeks and at least 1 active campaign (sum of budget 30 days > 0)	
    from seller_churn_tmp
    left outer join `etsy-data-warehouse-prod.rollups.prolist_daily_summary` as b on a.date = b.date 
    group by 1, 2, 3 
    order by 1 desc
;