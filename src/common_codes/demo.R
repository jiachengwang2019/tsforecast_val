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


#::::::::::::::::::: Prepare the data
source(here::here("src","common_codes","parameters_CENT.R"))
source(here::here("src","common_codes","data_prep_functions_v2.R"))
source(here::here("src","common_codes","assumption_checking_functions.R"))
source(here::here("src","common_codes","forecasting_functions.R"))
source(here::here("src","common_codes","model_evaluation.R"))
source(here::here("src","common_codes","plotting_functions.R"))

options(warn = -1,digits = 3,verbose = F,error = NULL)


# :::::::::::::::::: PART0 PREPARE THE DATA
# Setup the parameters 
# NOTCE: please make sure the net_type is consistent with parameters_CENT/BENT.R function
net_type = 'Cable'
net_ = 'USA'

inputpath = paste0('s3://nbcu-ds-linear-forecasting/processed/pacing/20W030/', net_ ,'_AFL_0420_200424_20W030.csv')
raw_data_AFL <- create_raw_data(inputFilePath = inputpath,
                                network = net_type,
                                group_by_cols = 'default')

aggdata <- create_aggregated_data (raw_data_AFL ,
                                   interactive_input = T,
                                   network = net_type,
                                   time_id =  'Week',
                                   agg_group_by_cols = 'default')

case_data <- aggdata$aggregated_data
case_data = case_data[order(case_data$Week),]

# :::::::::::::::::: PART1-CHECK ARIMA MODEL ASSUMPTION
st_check <- check_stationary(data = case_data, variable = 'SC3_Impressions')
st_check
