library(dplyr)
library(DBI)
library(ggplot2)
library(reshape2)
library(ddply)
library(mctest)
library(corpcor)
options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/analytics", 
                                        "https://www.googleapis.com/auth/webmasters"))
library(CausalImpact)
billing <- 'etsy-bigquery-adhoc-prod'
bq_auth(email = NA)

# load in the daily prolist data from 2017 to today. there are the normal daily prolist datapoints, along with some data on
# budget constrained status at various timeframes (daily, 3mo, 6mo, year). since BC status is volatile and can be dependent on
# visit volume, the various timeframes were investigated to see what is the most stable indicator of BC sellers increasing their budget
sql <- "select * from etsy-data-warehouse-dev.pdavidoff.bc_budget_and_revenue;"

tb_sid <- bq_project_query(billing, sql)
df_sid <- bq_table_download(tb_sid)

# filter down variables to some key ones that could predict revenue
filter_vars <- df_sid %>% 
  select(date,bc_3mo_budget,total_spend,non_bc_3mo_budget,bc_3mo_shops,non_bc_3mo_shops,cost_per_click,pccr,ctr,impressions_per_visit,etsy_visits) %>%
  mutate(bc_3mo_budget_per_seller = bc_3mo_budget/bc_3mo_shops)

filter_down_vars_budget <- filter_vars %>% select(date,bc_3mo_budget,total_spend,non_bc_3mo_budget,cost_per_click,pccr,ctr,impressions_per_visit) %>%
  filter(date >= "2022-01-01")

filter_down_vars_spend <- filter_vars %>% select(date,total_spend,bc_3mo_budget,non_bc_3mo_budget,cost_per_click,pccr,ctr,impressions_per_visit) %>%
  filter(date >= "2022-01-01")

# date_filter <- filter_down_vars %>% filter(date >= "2022-01-01" )

# set pre/post dates for the experiment. used three weeks to account for impact of experiment without introduction of
# confounders from other launches or changes in macro factors

pre <- as.Date(c("2022-03-01","2022-04-26"))
post <- as.Date(c("2022-04-27","2022-05-22"))

# impact modeling
impact <- CausalImpact(filter_down_vars_budget,pre,post)
impact <- CausalImpact(filter_down_vars_spend,pre,post)

summary(impact)
plot(impact)
