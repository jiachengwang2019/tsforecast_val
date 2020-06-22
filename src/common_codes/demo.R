# #######################################################################
#       File-Name:      TSForecast_demo_example.R
#       Version:        R 3.6.2
#       Date:           June 19, 2020 
#       Author:         Jiacheng Wang <Jiacheng.Wang@nbcuni.com>
#       Purpose:        examples related to TSForecast package 
#       Input Files:    NONE
#       Output Files:   NONE
#       Data Output:    NONE
#       Previous files: NONE
#       Dependencies:   NONE
#       Required by:    NONE
#       Status:         In Progress
#       Machine:        NBCU laptop
#  #######################################################################
library(here)

source(here::here('src', 'common_codes', 'assumption_checking_functions.R'))
source(here::here('src', 'common_codes', 'dependent_functions.R'))
source(here::here("src","common_codes","parameters_CENT.R"))
source(here::here("src","common_codes","data_prep_functions_v2.R"))
source(here::here("src","common_codes","assumption_checking_functions.R"))
source(here::here("src","common_codes","forecasting_functions.R"))
source(here::here("src","common_codes","model_evaluation.R"))
source(here::here("src","common_codes","plotting_functions.R"))

options(warn = -1,digits = 3,verbose = F,error = NULL)


# :::::::::::::::::: PART0 PREPARE THE DATA
# Setup the parameters 
# Notice: 
# please make sure the net_type is consistent with parameters_CENT/BENT.R function
net_type = 'Cable'
net_ = 'USA'

inputpath = paste0('s3://nbcu-ds-linear-forecasting/processed/pacing/20W030/', net_ ,'_AFL_0420_200424_20W030.csv')
raw_data_AFL <- create_raw_data(inputFilePath = inputpath,
                                network = net_type,
                                group_by_cols = 'default')

# In this example, I filter SL_daypart and HH_daypart both using SalesPrime.
aggdata <- create_aggregated_data (raw_data_AFL ,
                                   interactive_input = T,
                                   network = net_type,
                                   time_id =  'Week',
                                   agg_group_by_cols = 'default')

case_data <- aggdata$aggregated_data
case_data = case_data[order(case_data$Week),]

# :::::::::::::::::: PART1 - CHECK ARIMA MODEL ASSUMPTION
# STATIONARY CHECK
# This functions works as a pre-model-selection procedure and it does the following three things:
# 1. Perform 3 hypothesis testing on interested predictor variable, in this example, the interested variable is 'SC3_Impressions'
# 2. Give a suggestion differencing_order to remove the trend in the interested variable
# 3. Provide the time series plot for interested variable
#
# Remark:
# In the test_results, KPSS is a more conservative testing strategy compared to the other two tests, ADF and PP.
# For detailed introduction to stationary test, please refer to the following wiki page:
# ADF: https://en.wikipedia.org/wiki/Augmented_Dickey%E2%80%93Fuller_test
# KPSS: https://en.wikipedia.org/wiki/KPSS_test
# PP: https://en.wikipedia.org/wiki/Phillips%E2%80%93Perron_test

st_check <- check_stationary(data = case_data, log_transformation = 1, variable = 'SC3_Impressions')
st_check

# :::::::::::::::::: PART2 - FIND CHAMPION ARIMA MODEL

# Set up possible sarima model parameters candidates 
# Notice: 
# The parameter d in the argument max_arima_order(p,d,q) should take the advice from 'differencing_order' results of `check_stationary()`` function
weeklylookup <- create_arima_lookuptable(max_arima_order = c(2,1,2),
                                           max_seasonal_order = c(1,0,1),
                                           periods = c(52))

OOS_start = as.Date("2019-01-01")

wkly_regressors = c(
  "intercept","Jan","Feb", "Mar", "Apr", "May", 
  "Jun","Jul", "Aug", "Oct", "Nov", "Dec",
  "Easter_week_ind", "Memorial_Day_week_ind", "Independence_Day_week_ind", 
  "Halloween_week_ind", "Thanksgiving_week_ind", "Thanksgiving_week_aft_ind", 
  "Christmas_week_bfr_ind","Christmas_week_ind","New_Years_Eve_week_ind","trend"
)

keep_reg = c("Jan","Feb", "Mar", "Apr", "May", "Jun","Jul", "Aug", "Oct", "Nov", "Dec")

# Remark:
# 1. 'stream' is our interest predicted variable, i.e., 'SC3_Impression' in this example
# 2. Make sure 'agg_timescale' is consistent with time_id parameter in ' create_aggregated_data()' function
champion = find_champion_arima(data = case_data,
                               stream = 'SC3_Impressions',
                               agg_timescale = "Week",
                               log_transformation = 1,
                               OOS_start = OOS_start,
                               regressors = wkly_regressors,
                               keep_regressors = keep_reg,
                               max_changepoints = 0, # later also experiment with 3 changepoints
                               lookuptable = weeklylookup)

# ::::::::::::::: PART3 - CHECK ASSUMPTIONS FOR SELECTED CHAMPION ARIMA MODEL
# ::::::::::::::: Part 3.1 check normality residuals
# This functions works as a post-model selection arima model assumption check and it does the following three things:
# 1. Perform 3 hypothesis testing on interested predictor variable, in this example, the interested variable is 'SC3_Impressions'
# 2. Give a suggestion differencing_order to remove the trend in the interested variable
# 3. Provide the time series plot for interested variable
normal_check <- check_residual_normality(data = champion$champion_result)
normal_check
