#  #######################################################################
#       File-Name:      parameters.R
#       Version:        R 3.4.4
#       Date:           Mar 14, 2019
#       Author:         Soudeep Deb <Soudeep.Deb@nbcuni.com>
#       Purpose:        defines parameters that will be used in USA models
#       Input Files:    NONE
#       Output Files:   NONE
#       Data Output:    NONE
#       Previous files: NONE
#       Dependencies:   parameters.R
#       Required by:    MAIN.R
#       Status:         APPROVED
#       Machine:        NBCU laptop
#  #######################################################################

# :::::::::MODEL AND DATA DETAILS

current_model = "V1.1_190314"
current_data = "19W017"
inputFilePath = "/mnt/nbcu-ds-linear-forecasting/processed/pacing/19W025/USA_AFL_0422_190423_19W025.csv"
inputFileName = "USA_data"


# :::::::: GROUP_BY VARIABLES

group_by_cols <- c("Source","Network", "Demo", "HH_NT_Daypart","Broadcast_Year","Cable_Qtr","Week","Date","daylight_indi",
                   "Half_Hr","hourly","Show_Name","program_type","FirstAiring","Genre","Premiere","Season_num","Episode",
                   "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug","Sep","Oct", "Nov", "Dec","Labor_Day_ind","Columbus_Day_ind","New_Years_Eve_ind",
                   "Memorial_Day_week_ind","Memorial_Day_ind","Halloween_ind","Labor_Day_week_ind","Columbus_Day_week_ind",           
                   "Thanksgiving_ind","Christmas_week_ind","Christmas_ind","Christmas_week_bfr_ind","Halloween_ind", 
                   "Halloween_week_ind","Thanksgiving_week_ind","Independence_Day_week_ind","Easter_week_ind",             
                   "New_Years_Eve_week_ind","Easter_ind","Independence_Day_ind","Thanksgiving_week_aft_ind")

hh_group_by_cols <- c("Source","Network", "Demo", "HH_NT_Daypart","Broadcast_Year","Cable_Qtr","Week","Date","daylight_indi","hourly","Half_Hr",
                      "FirstAiring","Show_Name","program_type","Genre","Premiere","Season_num","Episode",
                      "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug","Sep","Oct", "Nov", "Dec", "Labor_Day_ind","Columbus_Day_ind","New_Years_Eve_ind",
                      "Memorial_Day_week_ind","Memorial_Day_ind","Halloween_ind", "Labor_Day_week_ind","Columbus_Day_week_ind",            
                      "Thanksgiving_ind","Christmas_week_ind","Christmas_ind","Christmas_week_bfr_ind",    
                      "Halloween_week_ind","Thanksgiving_week_ind","Independence_Day_week_ind","Easter_week_ind",             
                      "New_Years_Eve_week_ind","Easter_ind","Independence_Day_ind","Thanksgiving_week_aft_ind")

hourly_group_by_cols <- c("Source","Network", "Demo", "HH_NT_Daypart","Broadcast_Year","Cable_Qtr","Week","Date","daylight_indi","hourly",
                          "FirstAiring","Show_Name","program_type","Season_num","Episode",
                          "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                          "Jul", "Aug","Sep","Oct", "Nov", "Dec", "Labor_Day_ind","Columbus_Day_ind","New_Years_Eve_ind",
                          "Memorial_Day_week_ind","Memorial_Day_ind","Halloween_ind","Labor_Day_week_ind","Columbus_Day_week_ind",           
                          "Thanksgiving_ind","Christmas_week_ind","Christmas_ind","Christmas_week_bfr_ind",    
                          "Halloween_week_ind","Thanksgiving_week_ind","Independence_Day_week_ind","Easter_week_ind",             
                          "New_Years_Eve_week_ind","Easter_ind","Independence_Day_ind","Thanksgiving_week_aft_ind")

daily_group_by_cols <- c("Source","Network","Demo","HH_NT_Daypart","Broadcast_Year",
                         "Cable_Qtr","Week","Date","daylight_indi","Jan","Feb","Mar","Apr","May","Jun",
                         "Jul","Aug","Sep","Oct","Nov","Dec","Labor_Day_ind","Columbus_Day_ind",
                         "New_Years_Eve_ind","Memorial_Day_week_ind","Memorial_Day_ind","Halloween_ind",              
                         "Thanksgiving_ind","Christmas_week_ind","Christmas_ind","Christmas_week_bfr_ind","Labor_Day_week_ind",  
                         "Halloween_week_ind","Thanksgiving_week_ind","Independence_Day_week_ind","Easter_week_ind","Columbus_Day_week_ind",             
                         "New_Years_Eve_week_ind","Easter_ind","Independence_Day_ind","Thanksgiving_week_aft_ind")

weekly_group_by_cols <- c("Source","Network","Demo","HH_NT_Daypart","Broadcast_Year",
                          "Cable_Qtr","Week","Jan","Feb","Mar","Apr","May","Jun",
                          "Jul","Aug","Sep","Oct","Nov","Dec","Labor_Day_week_ind","Columbus_Day_week_ind",
                          "Memorial_Day_week_ind","Christmas_week_ind","Christmas_week_bfr_ind",    
                          "Halloween_week_ind","Thanksgiving_week_ind","Independence_Day_week_ind","Easter_week_ind",             
                          "New_Years_Eve_week_ind","Thanksgiving_week_aft_ind")

# :::::::::: VARIABLES TO KEEP IN THE INITIAL DATA FILTERING
keep_cols <- c("Source","Network", "Demo", "Broadcast_Year", "Cable_Qtr", "Date", "Week","daylight_indi",
               "Show_Name", "NBC_Show_Name", "ShowName_Orig","program_type","Genre","Premiere","Season_num","Episode",
               "Start_Time", "End_Time", "Half_Hr", "hourly", "HH_NT_Daypart","FirstAiring",
               "Tot_UE", "LS_Imps", "Nielsen_LS_Rating", "LS_Dur",
               "SC3_Impressions", "Nielsen_SC3_Rating", "SC3_C_Dur",
               "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
               "Sun","Mon","Tue","Wed","Thu","Fri","Sat", 
               "New_Years_Eve_ind","Memorial_Day_week_ind","Memorial_Day_ind","Halloween_ind","Labor_Day_ind","Columbus_Day_ind",              
               "Thanksgiving_ind","Christmas_week_ind","Christmas_ind","Christmas_week_bfr_ind","Labor_Day_week_ind","Columbus_Day_week_ind",
               "Halloween_week_ind","Thanksgiving_week_ind","Independence_Day_week_ind","Easter_week_ind",             
               "New_Years_Eve_week_ind","Easter_ind","Independence_Day_ind","Thanksgiving_week_aft_ind")

# ::::::::: VARIABLES TO KEEP IN THE OUTPUT DATA
output_cols <- c("NETWORK","DEMO","TIME_ID","SHOW_NAME","PROGRAM_TYPE","HH_NT_DAYPART","BROADCAST_YEAR","CABLE_QTR",
                 "WEEK","DATE","HOURS","HALF_HR","SC3_IMPRESSIONS","SC3_C_DUR","PREDICT","ERROR","APE")