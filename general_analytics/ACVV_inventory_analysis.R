library(bigrquery)
library("readr")
library(dplyr)
library(DBI)
library(ggplot2)
library(reshape2)
library(ddply)
options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/analytics", 
                                      "https://www.googleapis.com/auth/webmasters"))
billing <- 'etsy-bigquery-adhoc-prod'
bq_auth(email = NA)


sql <- "with visit_purchases as (
select
	distinct
	visit_id,
	query
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions`
where
	purchases > 0 and _date = current_date - 2
),base as (
select
	a.visit_id,
	a.query,
	a.page,
	a.listing_id,
	clicks,
	purchases,
	price_usd,
	impressions
from
	`etsy-data-warehouse-prod.search.visit_level_listing_impressions` a join
	`etsy-data-warehouse-prod.listing_mart.listings` b on a.listing_id = b.listing_id join
	visit_purchases c on a.visit_id = c.visit_id and a.query = c.query
where
	_date = current_date - 2
)
select
	visit_id,
	query,
	(sum(price_usd)/sum(impressions))/100 as price_per_impression,
	(sum(case when clicks > 0 then price_usd*clicks end)/sum(clicks))/100 as price_per_click,
	(sum(case when purchases > 0 then price_usd*purchases end)/sum(purchases))/100 as price_per_purchase
from
	base
group by 1,2
order by 1,2;
"

# look at density curves for single item orders for different categories
sql_sid <- "select
	a.receipt_id,
	top_category_gms,
	count(distinct transaction_id) as transaction_count,
	sum(quantity) as total_quantity,
	max(b.gms_net) as aiv
from
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` a join
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` b on a.receipt_id = b.receipt_id
where
	extract(date from a.creation_tsz) >= current_date - 90
group by 1,2
having count(distinct transaction_id) = 1 and sum(quantity) = 1
order by rand()
limit 1000000
;"


tb_sid <- bq_project_query(billing, sql_sid)
df_sid <- bq_table_download(tb_sid)
df_med <- df_sid %>% summarize(med_value = median(aiv, na.rm = TRUE))
df_summary <- df_sid %>% group_by(top_category_gms) %>% 
  summarize(median = median(aiv), mean = mean(aiv))

# density plot for separate categories
ggplot(df_sid,aes(x=aiv)) + geom_density() + xlim(0,100) + 
  geom_vline(aes(x=med_value),linetype = "dashed",size = 0.5)

# density plot for overall group. the median is around $17, and the mean 
# is around $26.
ggplot(df_sid,aes(x=aiv)) + geom_density() + xlim(0,100) + 
  geom_vline(aes(xintercept=median(aiv)),linetype = "dashed",color = "orange",size = 0.5) +
  geom_vline(aes(xintercept=mean(aiv)),linetype = "dashed",color = "blue",size = 0.5,scale_color_manual(labels = "mean",values = "blue")) +

# facet grid for all of the different categories
base_plot <- ggplot(df_sid,aes(x=aiv)) + geom_density() + xlim(0,100) + facet_wrap(top_category_gms ~ ., scales = "free_y") 

base_plot + geom_vline(df_summary,mapping = aes(xintercept=median,color = "orange"),linetype = "dashed") +
  geom_vline(df_summary,mapping = aes(xintercept=mean,color = "blue"),linetype = "dashed") +
  labs(title = "Mean and Median Price for Single Item Orders by Category",x = "AIV",y="Density",color = "Legend Title \n") +
  scale_color_manual(labels = c("mean","median"),values = c("orange","blue")) + theme_bw()

sql_density_listing <- "
	select listing_id,price_usd,ntile(10) over(order by price_usd) as inventory_pctile from `etsy-data-warehouse-prod.rollups.active_listing_basics`
	order by rand() limit 1000000
;
"
tb_listing_density <- bq_project_query(billing, sql_density_listing)
df_listing_density <- bq_table_download(tb_listing_density)



sql_density_buyer_action <- "with base as (
select
	a.receipt_id,
	top_category_gms as top_category,
	max(listing_id) as listing_id,
	count(distinct transaction_id) as trans_count,
	sum(quantity) as total_quantity,
	max(gms_net) as receipt_gms
from
	`etsy-data-warehouse-prod.transaction_mart.receipts_gms` a join
	`etsy-data-warehouse-prod.transaction_mart.all_transactions` b on a.receipt_id = b.receipt_id
where
	extract(date from a.creation_tsz) = '2021-12-12'
group by 1,2
having count(distinct transaction_id) = 1 and sum(quantity) = 1
)
select
	listing_id,
	price_usd,
	top_category,
	'listings' as var_source
from
	(select listing_id,price_usd,top_category from `etsy-data-warehouse-prod.rollups.active_listing_basics` order by rand() limit 2000000)
UNION ALL
select
  distinct
	a.listing_id,
	price_usd,
	(split(full_path, '.')[ORDINAL(1)]) as top_category,
	'listing views' as var_source
from
	(select listing_id,price_usd from `etsy-data-warehouse-prod.analytics.listing_views` where _date = '2021-12-12' and price_usd is not null order by rand() limit 2000000) a join
	`etsy-data-warehouse-prod.materialized.listing_taxonomy` b on a.listing_id = b.listing_id
UNION ALL
select
	listing_id,
	receipt_gms as price_usd,
	top_category,
	'purchases' as var_source
from
	(select * from base order by rand() limit 2000000)
UNION ALL
select
  1 as listing_id,
  imp_price_median as price_usd,
  query_top_category as top_category,
  'queries' as var_source
from
  `etsy-data-warehouse-dev.pdavidoff.aov_query_intent_groups`
where
  query_group_label = 'Direct'
order by rand() limit 2000000
;
"

tb_density_buyer_action <- bq_project_query(billing, sql_density_buyer_action)
df_density_buyer_action <- bq_table_download(tb_density_buyer_action)

df_density_filter_cat <- df_density_buyer_action %>% filter(!top_category %in% c('other')) %>% filter(!is.na(top_category))
top_etsy_categories <- df_density_filter_cat %>% filter(top_category %in% c('home_and_living','jewelry','craft_supplies_and_tools','clothing','art_and_collectibles','accessories','weddings','paper_and_party_supplies'))

# layer density plots for seller inventory and buyer actions
ggplot(df_density_filter_cat,aes(x=price_usd,group=var_source,fill=var_source)) + geom_density(adjust = 1.5, alpha = .3) + xlim(0,75) +
  labs(title = "Inventory, Direct Query, Listing View and Single-Item Purchase Price",x = "Item Price",y="Density",fill = "Legend Title \n") +
  facet_wrap(top_category ~ ., scales = "free_y") +
  theme_bw()

# top two categories only
ggplot(top_etsy_categories,aes(x=price_usd,group=var_source,fill=var_source)) + geom_density(adjust = 1.5, alpha = .3) + xlim(0,75) +
  labs(title = "Inventory, Direct Query, Listing View and Single-Item Purchase Price",x = "Item Price",y="Density",fill = "Legend Title \n") +
  facet_wrap(top_category ~ ., scales = "free_y") +
  theme_bw()

sql_density_buyer_query_price <- 
  "select
    query,
    query_top_category,
    buyer_segment,
    imp_price_median,
    case 
      when buyer_segment in ('Habitual') then 'Habitual'
      when buyer_segment in ('Repeat','High Potential') then 'Repeat'
      when buyer_segment in ('Active') then 'Active'
      when buyer_segment in ('Not Active','New') then 'Not Active'
      when buyer_segment = 'Signed Out' then buyer_segment
    end as buyer_segment_group,
    coalesce(search_attributed_click,0) as search_attributed_click,
    coalesce(search_attributed_cart_add,0) as search_attributed_cart_add,
    coalesce(search_attributed_purchase,0) as search_attributed_purchase,
    round(imp_price_median) as round_price,
    case 
      when imp_price_median <= 5 then 5
      when imp_price_median <= 10 then 10
      when imp_price_median <= 15 then 15
      when imp_price_median <= 20 then 20
      when imp_price_median <= 25 then 25
      when imp_price_median <= 30 then 30
      when imp_price_median <= 35 then 35
      when imp_price_median <= 40 then 40
      when imp_price_median <= 45 then 45
      when imp_price_median <= 50 then 50
      when imp_price_median <= 55 then 55
      when imp_price_median <= 60 then 60
      when imp_price_median <= 65 then 65
      when imp_price_median <= 70 then 70
      when imp_price_median <= 75 then 75
      when imp_price_median <= 80 then 80
      when imp_price_median <= 85 then 85
      when imp_price_median <= 90 then 90
      when imp_price_median <= 95 then 95
      when imp_price_median <= 100 then 100
      when imp_price_median > 100 then 101
    end as median_imp_price_group_5,
        case 
      when imp_price_median <= 10 then 10
      when imp_price_median <= 20 then 20
      when imp_price_median <= 30 then 30
      when imp_price_median <= 40 then 40
      when imp_price_median <= 50 then 50
      when imp_price_median <= 60 then 60
      when imp_price_median <= 70 then 70
      when imp_price_median <= 80 then 80
      when imp_price_median <= 90 then 90
      when imp_price_median <= 100 then 100
      when imp_price_median > 100 then 101
    end as median_imp_price_group_10

  from
    `etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
  where
    query_group_label = 'Direct'
  order by rand()
  limit 1000000
"

tb_density_buyer_segment <- bq_project_query(billing, sql_density_buyer_query_price)
df_density_buyer_segment <- bq_table_download(tb_density_buyer_segment)

df_filter_signed_out <- df_density_buyer_segment %>% filter(!buyer_segment %in% 'Signed Out')
df_summary_buyer_segment <- df_filter_signed_out %>% group_by(buyer_segment_group) %>% 
  summarize(median = median(imp_price_median))

ggplot(df_filter_signed_out,aes(x=imp_price_median,group=buyer_segment_group,fill=buyer_segment_group)) + geom_density(adjust = 1.5) + xlim(0,75) +
  labs(title = "Direct Query Median Price Density by Buyer Segment",x = "Median Price",y="Density",fill = "Legend \n") 

df_buyer_segment_purchase_rate <- df_filter_signed_out %>% group_by(buyer_segment_group,median_imp_price_group_10) %>% 
  summarize(mean = mean(search_attributed_purchase))

ggplot(df_buyer_segment_purchase_rate, aes(x=median_imp_price_group_10,y=mean,group = buyer_segment_group,color = buyer_segment_group)) +
  geom_line()

sql_query_scatter <- "
with base as (
select
	query,
	query_group_label,
	aov_supply_price_group,
	aov_supply_variability_group,
	query_top_category,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,	
	count(*) as query_sessions
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases`
where
  query_group_label = 'Direct'
group by 1,2,3,4,5
),row_base as (
select
	*
from
	(select *,row_number() over(partition by query order by query_sessions desc) as row from base)
where
	row = 1
),compile as (
select
	a.query,
	a.query_group_label,
	a.aov_supply_price_group,
	a.aov_supply_variability_group,
	a.query_top_category,
	approx_quantiles(imp_price_quartile_1,100)[OFFSET(50)] as quartile_1_price,
	approx_quantiles(imp_price_median,100)[OFFSET(50)] as median_price,
	approx_quantiles(imp_price_quartile_3,100)[OFFSET(50)] as quartile_3_price,
	count(distinct visit_id) as query_sessions,
	# sum(total_listing_views) as total_listing_views,
	count(case when search_attributed_click > 0 then visit_id end)/count(visit_id) as click_rate,
	count(case when search_attributed_cart_add > 0 then visit_id end)/count(visit_id) as cart_add_rate,
	count(case when search_attributed_purchase > 0 then visit_id end)/count(visit_id) as purchase_rate,
	sum(search_attributed_gms/100)/count(*) as gms_per_query
from
	`etsy-data-warehouse-dev.pdavidoff.aov_long_tail_purchases` a join
	row_base b on a.query = b.query and a.query_group_label = b.query_group_label and a.aov_supply_price_group = b.aov_supply_price_group and a.aov_supply_variability_group = b.aov_supply_variability_group and a.query_top_category = b.query_top_category and b.row = 1
group by 1,2,3,4,5
order by 9 desc
)
select
  *,
  row_number() over(order by query_sessions desc) as query_session_rank
from
  compile
;
"

tb_scatter_query <- bq_project_query(billing, sql_query_scatter)
df_query_scatter <- bq_table_download(tb_scatter_query)

df_scatter_formula <- df_query_scatter %>% filter(query_session_rank <= 10000) %>%
  mutate(log_mp = log(median_price+1))

#fit polynomial regression models up to degree 5

#create a scatterplot of x vs. y
p <- plot(df_scatter_formula$log_mp,df_scatter_formula$purchase_rate)
x <- df_scatter_formula$log_mp
y <- df_scatter_formula$purchase_rate
fit <- lm(y~poly(x,2,raw=TRUE))
# fit1 <- lm(y~I(x^2))data=df_scatter_formula)
# fit2 <- lm(purchase_rate~poly(log_mp,2,raw=TRUE), data=df_scatter_formula)
# fit3 <- lm(purchase_rate~poly(log_mp,3,raw=TRUE), data=df_scatter_formula)
# fit4 <- lm(purchase_rate~poly(log_mp,4,raw=TRUE), data=df_scatter_formula)
# fit5 <- lm(purchase_rate~poly(log_mp,5,raw=TRUE), data=df_scatter_formula)
fit1 <- lm(y~x)
fit2 <- lm(y~I(x^2))

pre1 <- predict(fit1,col="blue")
lines(pre1)

pre2 <- predict(fit2,col = "red")
lines(pre2)
fit2 <- lm(purchase_rate~poly(log_mp,2,raw=TRUE), data=df_scatter_formula)
fit3 <- lm(purchase_rate~poly(log_mp,3,raw=TRUE), data=df_scatter_formula)
fit4 <- lm(purchase_rate~poly(log_mp,4,raw=TRUE), data=df_scatter_formula)
fit5 <- lm(purchase_rate~poly(log_mp,5,raw=TRUE), data=df_scatter_formula)


pre <- predict(fit,col = "red")
lines(pre)

lines(df_scatter_formula$log_mp,predict(fit,data.frame(x = df_scatter_formula$log_mp)),col = "red")

plot(df_scatter_formula$log_mp, df_scatter_formula$purchase_rate, pch=19, xlab='x', ylab='y')

x_axis <- seq(1,15,length = 15)

lines(x_axis, predict(fit2, data.frame(x = x_axis)), col = 'blue')
summary(fit2)


summary(fit5)
#define x-axis values
x_axis <- seq(1, 15, length=15)

#add curve of each model to plot
lines(x_axis, predict(fit1, data.frame(x=x_axis)), col='green')
lines(x_axis, predict(fit2, data.frame(x=x_axis)), col='red')
lines(x_axis, predict(fit3, data.frame(x=x_axis)), col='purple')
lines(x_axis, predict(fit4, data.frame(x=x_axis)), col='blue')
lines(x_axis, predict(fit5, data.frame(x=x_axis)), col='orange')

lm(formula = purchase_rate~median_price,data = df_scatter_formula)

df_expected_purchase_rate <- df_scatter_formula %>% mutate(exp_purchase_rate = 0.0717812 + (median_price*-0.0003557)) %>%
  mutate(demand_perf= purchase_rate / exp_purchase_rate)

ggplot(df_expected_purchase_rate, aes(x = purchase_rate,y=exp_purchase_rate,label = query)) + geom_point()


scatter_query_top_filter <- df_query_scatter %>% filter(query_session_rank <= 1000) %>%
  mutate(qs_log = log(query_sessions+1)) %>% mutate(pr_log = log(purchase_rate+1))


scatter_query_top_filter <- df_query_scatter %>% filter(query_session_rank <= 1000) %>%
  mutate(qs_log = log(query_sessions+1)) %>% mutate(pr_log = log(purchase_rate+1))


list_plots <- lapply(scatter_query_top_filter[-1], function(data) 
  ggplot(scatter_query_top_filter, aes(x= qs_log, y = scatter_query_top_filter, colour="green", label=query))+
    geom_point() +
    geom_text(aes(label= ifelse(data > quantile(data, 0.95),
                                as.character(Name),'')),hjust=0,vjust=0))

ggplot(scatter_query_top_filter, aes(x = qs_log,y=purchase_rate,label = query)) + geom_point()


df_med <- df_sid %>% summarize(med_value = median(aiv, na.rm = TRUE))

melt_df <- melt(df,id=c("visit_id","query"))
filter_melted_data <- melt_df %>% filter(value <= 50)

ggplot(filter_melted_data, aes(x=value,fill=variable)) + geom_density(alpha = 0.3)

tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb)

shop_inventory_density_by_tier <- "select
	listing_id,
	case
		when b.past_year_gms = 0 then 'active seller'
		when b.past_year_gms > 0 and seller_tier not in ('top seller','power seller') then 'sws'
		else seller_tier
	end as seller_tier,
	price_usd,
	top_category_new,
	case when c.name in ('United States','Germany','United Kingdom','Australia',
	'France') then c.name else 'ROW' end as country_name
from
	`etsy-data-warehouse-prod.rollups.active_listing_basics` a join
	`etsy-data-warehouse-prod.rollups.seller_basics` b on a.user_id = b.user_id join
	`etsy-data-warehouse-prod.etsy_v2.countries` c on b.country_id = c.country_id
order by rand()
limit 5000000
;"


tb_shop_density <- bq_project_query(billing, shop_inventory_density_by_tier)
df_shop_density <- bq_table_download(tb_shop_density)

df_med <- df_shop_density %>% summarize(med_value = median(price_usd, na.rm = TRUE))
df_summary <- df_shop_density %>% group_by(seller_tier) %>% 
  summarize(median = median(price_usd), mean = mean(price_usd))


# density curves for seller inventory by seller tier
ggplot(df_shop_density,aes(x=price_usd,group=seller_tier,fill=seller_tier)) + geom_density(adjust = 1.5, alpha = .3) + xlim(0,75) +
  labs(title = "Active Inventory Price Distribution by Seller Tier",x = "Item Price",y="Density",fill = "Legend Title \n")

# density curves for different countries for top and power sellers
df_shop_density_tp <- df_shop_density %>% filter(seller_tier %in% c('top seller','power seller'))
df_summary_shop <- df_shop_density %>% group_by(country_name) %>% 
  summarize(median = median(price_usd), mean = mean(price_usd))

sd_p <- ggplot(df_shop_density_tp,aes(x=price_usd)) + geom_density(adjust = 1.5, alpha = .3) + xlim(0,100) +
  labs(title = "Top and Power Active Inventory Price Distribution by Country",x = "Item Price",y="Density",fill = "Legend Title \n") +
  facet_wrap(country_name ~ ., scales = "free_y")
  
sd_p + geom_vline(df_summary_shop,mapping = aes(xintercept = median, color="orange"),linetype = "dashed") +
  geom_vline(df_summary_shop,mapping = aes(xintercept=mean,color = "blue"),linetype = "dashed") +
  # labs(title = "Mean and Median Price for Single Item Orders by Category",x = "AIV",y="Density",color = "Legend Title \n") +
  scale_color_manual(labels = c("mean","median"),values = c("orange","blue")) + theme_bw()


# look at very high queries in a quadrant. Based on how often buyers search for that query and 
# it's GMS performance'

supply_demand_curve_perf <- "
with base as (
select
	query,
	query_group_label,
	aov_supply_price_group,
	aov_supply_variability_group,
	query_top_category,
	query_sessions,
	quartile_1_price,
	median_price,
	quartile_3_price,
	click_rate,
	cart_add_rate,
	purchase_rate,
	exp_purchase_rate_orig,
	0.0218330 + (-0.0217984*log_mp) + (0.0019986*pow(log_mp,2)) + (0.2320834*(habitual_rate+high_potential_rate+repeat_rate)) as exp_purchase_rate_new,
	gms_per_query,
	purchase_rate_perf_orig,
    log_mp,
	top_seller_share,
	star_seller_share,
	habitual_rate+high_potential_rate+repeat_rate as repeat_buyer_rate,
	personalizable_share,
	avg_giftiness_score,
	avg_query_group_label_score
from
	`etsy-data-warehouse-dev.pdavidoff.query_curve_summary`
)
select
  *,
  case when purchase_rate = 0 then 0 else purchase_rate/exp_purchase_rate_new - 1 end as purchase_rate_perf_new
from
  base
;
"

supply_demand_tb <- bq_project_query(billing, supply_demand_curve_perf)
df_supply_demand <- bq_table_download(supply_demand_tb)
df_very_high_filter <- df_supply_demand %>% filter()
lm_new = (lm(formula = purchase_rate ~ log_mp + repeat_buyer_rate+ I(log_mp^2) + I(log_mp^3), data = df_supply_demand))
summary(lm_new)
plot(lm_new$residuals, pch = 16, col = "red")


pressure <- read_excel("pressure.xlsx") #Upload the data

lmTemp = lm(Pressure~Temperature, data = pressure) #Create the linear regression
plot(pressure, pch = 16, col = "blue") #Plot the results
abline(lmTemp) #Add a regression line

df_very_high_filter <- df_supply_demand %>% filter(aov_supply_price_group == 'Very High')


ggplot(df_very_high_filter,aes(log_qs,purchase_rate_perf)) + ylim(-1,3) + xlim = (8,11) + geom_point()

