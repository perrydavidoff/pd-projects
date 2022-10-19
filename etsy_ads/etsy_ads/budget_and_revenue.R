# load the packages to build out the model
library(bigrquery)
library("readr")
library(dplyr)
library(DBI)
library(ggplot2)
library(reshape2)
library(ddply)
library(mctest)
library(corpcor)
options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/analytics", 
                                        "https://www.googleapis.com/auth/webmasters"))
billing <- 'etsy-bigquery-adhoc-prod'
bq_auth(email = NA)

# load in the daily prolist data from 2017 to today. there are the normal daily prolist datapoints, along with some data on
# budget constrained status at various timeframes (daily, 3mo, 6mo, year). since BC status is volatile and can be dependent on
# visit volume, the various timeframes were investigated to see what is the most stable indicator of BC sellers increasing their budget
sql <- "select * from etsy-data-warehouse-dev.pdavidoff.bc_budget_and_revenue;"

tb_sid <- bq_project_query(billing, sql)
df_sid <- bq_table_download(tb_sid)

# filter variables to the ones we want to investigate. certain variables were investigated before getting removed from the filter statement below.
# variables removed: daily, 6mo, year BC status, total visits, converting clicks, total budget.
filter_vars <- df_sid %>% 
  select(total_spend,bc_3mo_budget,non_bc_3mo_budget,bc_3mo_shops,non_bc_3mo_shops,cost_per_click,pccr,ctr,impressions_per_visit,total_visits) %>%
  mutate(bc_3mo_budget_per_seller = bc_3mo_budget/bc_3mo_shops)

# check for collinearity. generally, variables with consisted collinearity of 0.6+ across multiple variables were removed
cor(filter_vars, method = "pearson")

# regression model to predict total spend. R-squared is 0.9142.
reg_model = lm(formula = total_spend ~ bc_3mo_budget + non_bc_3mo_budget + cost_per_click + pccr + ctr + impressions_per_visit,filter_vars)

summary(reg_model)

