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
source(here::here("src","USA","parameters.R"))
source(here::here("src","common_codes","data_prep_functions_v2.R"))
source(here::here("src","common_codes","assumption_checking_functions.R"))
source(here::here("src","common_codes","forecasting_functions.R"))
source(here::here("src","common_codes","model_evaluation.R"))
source(here::here("src","common_codes","plotting_functions.R"))

options(warn = -1,digits = 3,verbose = F,error = NULL)

# :::::::::::::::::: PART1-CHECK ARIMA MODEL ASSUMPTION 
