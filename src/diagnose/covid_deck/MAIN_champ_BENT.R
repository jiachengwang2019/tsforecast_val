library(here)
library(RMySQL)
library(dplyr)

source(here::here("src","common_codes","parameters_BENT.R"))
source(here::here("src","common_codes","data_prep_functions_v2.R"))
source(here::here("src","common_codes","forecasting_functions.R"))
source(here::here("src","common_codes","model_evaluation.R"))
source(here::here("src","common_codes","plotting_functions.R"))
source(here::here('src', '4nets', 'dates.R'))

olympics_dates <- c('2016-08-01', '2016-08-15', '2021-07-20', '2021-08-10')
net_sb_dates <- nbc_sb_dates
other_sb_dates <- c(fox_sb_dates, cbs_sb_dates)
options(warn = -1,digits = 3,verbose = F,error = NULL)

#::::::::::::::::::::::::::::: READ THE MODEL DETAILS
prev_net = '0'

refresh = 'LRP'
add_LS  = F
filter_data = T
str_    = 'L_Imps'
net_type = 'Broadcast'
end_LS   = as.Date('2020-04-16')
net_='NBC'

OOS_start = as.Date('2019-07-01')  #as.Date('2019-06-01')  #

dailylookup <- create_arima_lookuptable(max_arima_order = c(4,0,4),
                                        max_seasonal_order = c(1,0,1),
                                        periods = c(52))
# 
# dailylookup <- create_arima_lookuptable(max_arima_order = c(1,0,0),
#                                          max_seasonal_order = c(0,0,0),
#                                          periods = c(52))


#Read showlist
SparkR::sparkR.session()
sc <- spark_connect(method = "databricks")
df_source <- spark_read_csv(sc, name = "show_data",
                            path = 's3://nbcu-msi-nielsen/raw/nielsen_mit_acm/extra_nielsen_data/Rickentx.csv',
                            delimiter = "|")
df_shows <- collect(df_source)
showlist  <- unique((df_shows %>% filter(network == 'NBC'))$nielsen_showname)

# set-up spark connection

dets_list  <- list()
report_list <- list()
all_details <- list()

#gen_weekly_group_by_cols = gen_weekly_group_by_cols[! gen_weekly_group_by_cols %in% c('SL_NT_Daypart')]
#gen_weekly_group_by_cols = c(gen_weekly_group_by_cols,"Olympics")

#monthly_reg = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Oct", "Nov", "Dec",  "Sun", "Mon", "Tue", "Thu", "Fri", "Sat") 
monthly_reg = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Oct", "Nov", "Dec") 

hol_reg = c("Easter_week_ind", "Memorial_Day_week_ind", "Independence_Day_week_ind", 
            "Halloween_week_ind", "Thanksgiving_week_ind", "Thanksgiving_week_aft_ind", 
            "Christmas_week_bfr_ind","Christmas_week_ind","New_Years_Eve_week_ind")
#ARIMA setup
all_regs   = c(monthly_reg, hol_reg,  'trend', 'YoY', 'GOLDENG', 'OLYMPICS', 'SUPERB') #'YoY',
keep_regs  = c(monthly_reg, "trend") 


#inputpath = "s3://nbcu-ds-linear-forecasting/processed/pacing/20W022/NBC_AFL_0330_200331_20W022.csv"

inputpath = paste0('s3://nbcu-ds-linear-forecasting/processed/pacing/20W029/', net_ ,'_AFL_0420_200423_20W029.csv')


#inputpath = paste0('s3://nbcu-ds-linear-forecasting/processed/pacing/20W021/', data_model$network ,'_AFL_0323_200325_20W021.csv')

raw_data_AFL <- create_raw_data(inputFilePath = inputpath,
                                network = net_type,
                                group_by_cols = 'default')

raw_data_AFL$raw_data$Date <- as.Date(raw_data_AFL$raw_data$Date)
raw_data_AFL$raw_data$Week <- as.Date(raw_data_AFL$raw_data$Week)

raw_data_AFL$raw_data <- raw_data_AFL$raw_data %>% filter(Date <= as.Date('2020-02-01'))

dayparts <- unique(raw_data_AFL$raw_data$SL_NT_Daypart)

dayparts <- dayparts[!is.na(dayparts)]

dayparts <- c("Prime" , "Late Night"  ,  "Daytime"  )

i = 1
for (dp in dayparts){
  
  
  if (dp == 'Prime'){
    
    
    #if (data_model$filter_show_name != "") filter_sh = data_model$filter_show_name else filter_sh = NA
    filt_raw <-    raw_data_AFL
    filt_raw$raw_data <- filt_raw$raw_data  %>% filter((ShowName_Orig %in% showlist & Source =='Nielsen') | (Source =='Compass'))  
    filt_raw$raw_data <- filt_raw$raw_data  %>% filter(!(Genre %in% c('SA','SC','SE','SN','N'))) 
    #nique((raw_data_AFL$raw_data %>% filter(grepl('BMA', Show_Name)))$Week)
    # unique((raw_data_AFL$raw_data %>% filter(startsWith(Show_Name, 'TNF')))$Show_Name)
    #head(arrange(raw_data_AFL$raw_data %>% filter(Broadcast_Year == 2019), desc(L_Rating)), n= 40) %>% View()
    
    
    aggdata <- create_aggregated_data (filt_raw ,
                                       interactive_input = F,
                                       filter_sl_nt_daypart = dp,
                                       filter_hh_nt_daypart = NA,
                                       filter_program_type = NA,
                                       filter_show_name = NA,
                                       filter_date = NA,
                                       filter_half_hr = NA,
                                       filter_cols = NA,
                                       network = net_type,
                                       time_id =  'Week',
                                       agg_group_by_cols = 'default')
    
    case_data <- aggdata$aggregated_data
  } else {
    aggdata <- create_aggregated_data (raw_data_AFL ,
                                       interactive_input = F,
                                       filter_sl_nt_daypart = dp,
                                       filter_hh_nt_daypart = NA,
                                       filter_program_type = NA,
                                       filter_show_name = NA,
                                       filter_date = NA,
                                       filter_half_hr = NA,
                                       filter_cols = NA,
                                       network = net_type,
                                       time_id =  'Week',
                                       agg_group_by_cols = 'default')
    
    case_data <- aggdata$aggregated_data
    
    
    
    
  }
  
  
  
  if (filter_data == T) case_data <- case_data %>% filter(Week > as.Date('2014-01-01'))
  
  
  #case_data = case_data[order(case_data$Week,case_data$Date),]
  case_data = case_data[order(case_data$Week),]
  
  #regtxt <- gsub(pattern = " ",replacement = "",x = data_model$regressors)
  #regset <- unlist(strsplit(regtxt,split = ","))
  #regset <- regset[!(regset %in% c("Labor_Day_ind" ))]
  
  case_data <- case_data %>%
    mutate(
      YoY = Broadcast_Year - 2013,
      LS_Jan_2015 = ifelse(Week >= as.Date('2015-01-01'), 1,0),
      LS_Q2_2015  = ifelse(Week >= as.Date('2015-03-30'), 1, 0),
      LS_Jan_2017 = ifelse( Week >= as.Date('2017-01-01'), 1, 0),
      Rebrand_LS_2017 = ifelse(Week >= as.Date('2017-10-01'), 1 ,0),
      LS_Sep_2018 = ifelse(Week > as.Date('2018-09-24'), 1, 0 )
      # LS_Cov =  ifelse(Week>= as.Date('2020-03-16') & Week <= end_LS, 1, 0)
    )
  
  case_data <- case_data %>%
    mutate(
      GRAMMYS = ifelse(.$Week %in% as.Date(grammy_dates),1,0),
      GOLDENG = ifelse(.$Week %in% as.Date(gg_dates),1,0),
      SUPERB  = ifelse(.$Week %in% as.Date(net_sb_dates),1,0),
      OTHERSB = ifelse(.$Week %in% as.Date(other_sb_dates),1,0),
      OSCARS  = ifelse(.$Week %in% as.Date(oscar_dates),1,0),
      ELECTIONS  = ifelse(.$Week %in% as.Date(elections_dates), 1 ,0),
      OLYMPICS   = ifelse(.$Week %in% as.Date(olympics_dates), 1 ,0),
      W_OLYMPICS = ifelse (.$Week %in% as.Date(win_olympics_dates), 1, 0),
      LS_3Q19 = ifelse(Broadcast_Year == 2019 & Broadcast_Qtr == '3Q',1,0),
      BIGBANG = ifelse(.$Week %in% as.Date(bigbang_dates), 1, 0),
      MASKED  = ifelse(.$Week %in% as.Date(masked_dates),   1, 0),
      AMERICANI = ifelse(.$Week %in% as.Date(american_dates), 1, 0),
      YoY =  Broadcast_Year - 2014,
      VOD = ifelse( Week >= as.Date("2017-03-27") & Week <= as.Date('2019-06-24'), 1 ,0),
      Q_IND = ifelse(Broadcast_Year > 2015, 1, 0),
      WWE = ifelse(Week >= as.Date("2019-10-04"),1,0)
    )
  
  
  #if ('intercept' %in% regset) keep_int = T else keep_int = F
  result <- find_champion_arima(data = case_data,
                                stream = str_, #data_model$stream,
                                agg_timescale = 'Week',
                                log_transformation = 1, # as.numeric(data_model$log_transformation),
                                use_boxcox = T,
                                OOS_start = OOS_start,
                                keep_intercept = T ,#keep_int,
                                regressors = all_regs,
                                keep_regressors = keep_regs,
                                max_changepoints = 2,
                                changepoints_minseglen = 20,
                                lookuptable = dailylookup)
  
  
  print(dp)
  
  print(result$champion_model)
  
  mape_ <- find_MAPE(result$champion_result,
                     show_variable = str_,
                     weight_variable = "L_Dur",
                     network = net_type,
                     timescale = 'Week',
                     OOS_start ,
                     Last_Actual_Date = as.Date("2099-12-31"))
  print(mape_)
  
  report <- data.frame(network = 'NBC',
                       daypart = dp,
                       #model_version = data_model$model_final_version, 
                       time = 'Week' ,
                       train_mape_daily = mape_[1,1],
                       test_mape_daily  = mape_[2,1],
                       train_mape_qtr   = mape_[1,2],
                       test_mape_qtr    = mape_[2,2],
                       regressors = result$regressors, 
                       changepoints = result$changepoints)
  
  
  print(weekly_forecast_plot(full_data_champion = result$champion_result,
                             show_variable = str_,
                             OOS_start = OOS_start,
                             title = paste0('NBC ,', dp)))
  
  
  dets <- create_model_details(result,
                               interactive_input = F, 
                               n = 1, 
                               data_prep_details = aggdata$data_prep_details,
                               model_type = 'DP_ALL_W', 
                               model_version = 'L2', 
                               model_previous_version = data_model$model_final_version, 
                               model_hierarchy = 'HH_NT_DAYPART, WEEK', 
                               forecast_rule = NA)
  
  #new_row <- update_mdt(data_model, new_model, model_version = mv_, n = 1)
  dets$cycle   = 'B'
  dets$refresh = 'LRP'
  dets$filter_date = "(Date; >= ; '2014-01-01')"
  
  dets_list[[i]]   <- dets
  report_list[[i]] <- report
  all_details[[i]] <- result$all_model_details
  
  prev_net = data_model$network 
  
  i = i + 1
  
}

mdt <- do.call(rbind, dets_list)
test_report <- do.call(rbind, report_list)
all_details_report <- do.call(rbind, all_details)

write.csv(mdt, here::here('src', 'diagnose', 'covid_deck', 'new_mdt_week_BENT_v3.csv'), row.names = F, na = "")
write.csv(test_report, here::here('src', 'diagnose', 'covid_deck', 'report_lrp_week_BENT.csv'), row.names = F, na = "")
write.csv(all_details_report, here::here('src', 'diagnose','covid_deck', 'all_details_lrp_week_BENT.csv'), row.names = F, na = "")