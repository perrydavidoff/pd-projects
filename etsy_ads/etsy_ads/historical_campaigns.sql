select
	status,
	count(distinct shop_id) as shop_count
from
	`etsy-data-warehouse-prod.etsy_shard.prolist_campaign`
group by 1
order by 2 desc
;
