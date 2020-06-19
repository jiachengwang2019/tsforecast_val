library(here)
library(RMySQL)
library(dplyr)

source(here::here("src","common_codes","parameters_CENT.R"))
source(here::here("src","common_codes","data_prep_functions_v2.R"))
source(here::here("src","common_codes","forecasting_functions.R"))
source(here::here("src","common_codes","model_evaluation.R"))
source(here::here("src","common_codes","plotting_functions.R"))

options(warn = -1,digits = 3,verbose = F,error = NULL)

#::::::::::::::::::::::::::::: READ THE MODEL DETAILS
prev_ver = '0'

refresh = 'LRP'
add_LS = F
str_ = 'L_Imps'
net_type = 'Cable'

net_list <- list('USA', 'BRVO', 'ENT', 'SYFY', 'OXYG', 'NBC')
net_ = 'OXYG'


# set-up spark connection
SparkR::sparkR.session()
sc <- sparklyr::spark_connect(method = "databricks")

gen_weekly_group_by_cols = gen_weekly_group_by_cols[! gen_weekly_group_by_cols %in% c('SL_NT_Daypart')]

#connection <- dbConnect(MySQL(), user = 'forecast_master', password = 'Line#rForec#st#1221', 
#                        host = 'linear-forecasting-prod.cqwbwa84updc.us-east-1.rds.amazonaws.com', dbname = 'linear_forecasting_outputs')
#model_details <- DBI::dbGetQuery(connection,"SELECT * FROM LRPmodelDetails WHERE current_prod = 1")
stringsAsFactors = FALSE
model_details <- read.csv(here::here('src', 'diagnose', "covid_deck", 'new_mdt_week.csv'))

model_details = model_details %>% filter(network == net_)
network = net_

output_cols <- c("NETWORK","DEMO","LAST_UPDATE","LATEST_BASE_DATA","FORECAST_DATA",
                 "TIME_ID","BROADCAST_YEAR","CABLE_QTR","QTRYEAR","HH_NT_DAYPART","STREAM","ACTUAL","PREDICT", 
                 "LAST_ACTUAL", 'BROADCAST_WEEK', "BROADCAST_DATE", 'L_DUR') #, #"MODEL_FINAL_VERSION",

#inputpath = paste0('s3://nbcu-ds-linear-forecasting/processed/pacing/20W029/', net_ ,'_AFL_0420_200423_20W029.csv')

inputpath = paste0('s3://nbcu-ds-linear-forecasting/processed/pacing/20W030/', net_ ,'_AFL_0420_200424_20W030.csv')

raw_data_AFL <- create_raw_data(inputFilePath = inputpath,
                                network = net_type,
                                group_by_cols = 'default')

#raw_data_AFL$raw_data$L_Imps <- raw_data_AFL$raw_data$L_Imps /2 
#raw_data_AFL$raw_data$L_Dur <- raw_data_AFL$raw_data$L_Dur / 2 

test <- raw_data_AFL$raw_data %>% filter(HH_NT_Daypart == 'SalesPrime')
tail(test$Date[!is.na(test$L_Imps)],1)

add_LS = T
OOS_start =  as.Date('2020-04-17') #as.Date('2020-02-01')  #
end_LS    =  as.Date('2020-04-30')   #as.Date('2020-06-30') #as.Date('2020-04-30')     

{
  
  all_outputs = list()
  results = list()
  
  # caseid=1
  for (caseid in 1:nrow(model_details)){
    
    data_model <- model_details[caseid,]
    
    all_outputs[[caseid]] = data.frame(matrix(nrow = 0,ncol = length(output_cols)))
    names(all_outputs[[caseid]]) = output_cols
    
    
    data_model$filter_hh_nt_daypart <- as.character(data_model$filter_hh_nt_daypart)
    data_model$filter_show_name <- as.character(data_model$filter_show_name)
    data_model$changepoints <- as.character(data_model$changepoints)
    data_model$regressors <- as.character(data_model$regressors)
    
    if (data_model$filter_show_name != "") filter_sh = data_model$filter_show_name else filter_sh = NA
    
    
    aggdata <- create_aggregated_data (raw_data_AFL ,
                                       interactive_input = F,
                                       filter_sl_nt_daypart = NA,
                                       filter_hh_nt_daypart = data_model$filter_hh_nt_daypart ,
                                       filter_program_type = NA,
                                       filter_show_name = filter_sh,
                                       filter_date = NA,
                                       filter_half_hr = NA,
                                       filter_cols = NA,
                                       network = net_type,
                                       time_id =  data_model$time_id,
                                       agg_group_by_cols = 'default')
    
    case_data <- aggdata$aggregated_data
    case_data = case_data[order(case_data$Week),]
    
    #insample_end <- max(subset(case_data,Source == "Nielsen")$Date)
    insample_end <- as.Date('2020-03-31')
    
    regtxt <- gsub(pattern = " ",replacement = "",x = data_model$regressors)
    regset <- unlist(strsplit(regtxt,split = ","))
    regset <- regset[!(regset %in% c("Labor_Day_ind" ))]
    
    # get the arima and seasonal orders
    arima_pdq <- as.numeric(data_model[,c("arima_p","arima_d","arima_q")])
    arima_seasonal <- list(order = as.numeric(data_model[,c("seasonal_p","seasonal_d","seasonal_q")]),
                           period = as.numeric(data_model$arima_period))
    
    # get the changepoint dates
    if (is.na(data_model$changepoints) == T | str_length(data_model$changepoints) < 2){
      cp_dates <- NA
    } else {
      if (grepl("/",data_model$changepoints) == T){
        date_parts = unlist(strsplit(data_model$changepoints,split = "/"))
        if (str_length(date_parts[3]) == 2) date_parts[3] = paste(c("20",date_parts[3]),collapse = "")
        cp_dates <- as.Date(paste(date_parts[c(3,1,2)],collapse = "-"))
      } else if (grepl("-",data_model$changepoints) == T){
        cp_dates <- as.Date(unlist(strsplit(data_model$changepoints,",")))
      }
    }
    
    
    case_data <- case_data %>%
      mutate(
        YoY = Broadcast_Year - 2013,
        LS_Jan_2015 = ifelse(Week >= as.Date('2015-01-01'), 1,0),
        LS_Q2_2015  = ifelse(Week >= as.Date('2015-03-30'), 1, 0),
        LS_Jan_2017 = ifelse( Week >= as.Date('2017-01-01'), 1, 0),
        Rebrand_LS_2017 = ifelse(Week >= as.Date('2017-10-01'), 1 ,0),
        LS_Sep_2018 = ifelse(Week > as.Date('2018-09-24'), 1, 0 ),
        #LS_Cov =  ifelse(Week >= as.Date('2020-03-16') & Week <= end_LS, 1, 0)
        LS_Cov = case_when(
          Week < as.Date('2020-03-16') ~ 0,
          Week == as.Date('2020-03-16') ~ 0.14,
          Week == as.Date('2020-03-23') ~ 0.77,
          Week > as.Date('2020-03-23') & Week <= end_LS ~ 0.95,
          Week > end_LS ~ 0
        )
        
        
      )
    #plot(case_data$LS_Cov)
    
    
    
    if (add_LS == T) regset <- c(regset, 'LS_Cov')
    
    # get the results from fitting the best ARIMA model
    results[[caseid]] <- fit_champion_arima(data = case_data,
                                            stream = str_, #data_model$stream,
                                            agg_timescale = data_model$time_id,
                                            log_transformation = as.numeric(data_model$log_transformation),
                                            boxcox = 'auto', #data_model$boxcox,
                                            OOS_start = OOS_start,
                                            regressors = regset,
                                            changepoint_dates = cp_dates,
                                            ARIMA_order = arima_pdq,
                                            ARIMA_seasonal = arima_seasonal)
    
    
    champ = results[[caseid]]$champion_result
    
    all_outputs[[caseid]] = champ %>%
      mutate(
        LAST_UPDATE = Sys.Date(), # format(Sys.time(),tz = "America/New_York",usetz = T),
        LAST_ACTUAL = insample_end,
        QTRYEAR = paste0(Broadcast_Year, "-", Cable_Qtr),
        MODEL_TYPE = data_model$model_type,
        MODEL_FINAL_VERSION = data_model$model_final_version,
        FORECAST_DATA = data_model$forecast_data,
        LATEST_BASE_DATA = data_model$latest_base_data,
        TIME_ID = data_model$time_id,
        STREAM = str_ , # data_model$stream,
        HH_NT_Daypart = ifelse(rep("HH_NT_Daypart",nrow(champ)) %in% names(champ),HH_NT_Daypart,NA)
      ) %>%
      rename(
        BROADCAST_WEEK = Week,
        L_DUR = L_Dur
      )
    if (data_model$time_id == 'Date') {
      all_outputs[[caseid]] = all_outputs[[caseid]] %>%
        rename(
          BROADCAST_DATE = Date
        )
    }
    
    if (data_model$time_id == 'Week'){
      all_outputs[[caseid]] = all_outputs[[caseid]] %>%
        mutate(
          BROADCAST_DATE =  BROADCAST_WEEK
        )
    }  
    
    
    names(all_outputs[[caseid]])[which(names(all_outputs[[caseid]]) == data_model$stream)] = "ACTUAL"
    all_outputs[[caseid]] = setNames(all_outputs[[caseid]], toupper(names(all_outputs[[caseid]])))
    
    
    
  }
  
  full_output <- do.call(rbind,lapply(all_outputs,function(x) return(x[,output_cols])))
  
  #lapply(all_outputs,function(x) return(print( setdiff(output_cols,colnames(x)) )))
  
  full_output$HH_NT_DAYPART[which(full_output$NETWORK == 'USA' & full_output$HH_NT_DAYPART == 'LateNight')] = 'SalesPrime'
  
  namesave = 0
  if (add_LS == T){
    namesave = paste0(net_,'_lrp_LS',end_LS, '.csv')
    full_output <- full_output %>%
      rename(PREDICT_LS = PREDICT)
    
  } else if (OOS_start == as.Date('2020-04-17')) {
    namesave = paste0(net_,'_lrp_OOS.csv')
  } else if (OOS_start == as.Date('2020-02-01')) {   
    namesave = paste0(net_,'_lrp.csv')
  }
  
  print(namesave)
  #write.csv(get_table(result$champion_result), here::here('src', 'diagnose', 'lrp_results', paste0('results_', net_, '.csv')))
  write.csv(full_output, here::here('src', 'diagnose', 'covid_deck','results', namesave))
  
}