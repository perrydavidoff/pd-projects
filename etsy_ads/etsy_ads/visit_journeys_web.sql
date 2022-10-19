select
	landing_path,
	count(*)/sum(count(*)) over() as visit_share
from
	`etsy-data-warehouse-dev.pdavidoff.visit_journeys_web`
where
	landing_path like "%shop_home%"
group by 1
order by 2 desc
;