--load data local inpath "/users/hdpcsclib/CSCLIB/QuarterlyCommitVSFcst/Quarterly Commit vs. Fcst.csv" OVERWRITE into table csclibrary_staging.MOS_WEEKLY_FORECAST_VS_PROJECTIONS_STG;

create table csclibrary_staging.MOS_WEEKLY_FORECAST_VS_PROJECTIONS_STG
(
Commodity	string,
Comments	string,
Pending_Spend	string,
Pending_Savings_pct	string,
Pending_Savings	string,
Base_Spend_Plan_No_Derate	string,
Savings_pct_Plan_No_Derate	string,
Savings_Plan_No_Derate	string,
Base_Spend_Recommit_No_Derate	string,
Savings_pct_Recommit_No_Derate	string,
Savings_Recommit_No_Derate	string,
Base_Spend_Forecast_No_Derate	string,
Savings_pct_Forecast_No_Derate	string,
Savings_Forecast_No_Derate	string,
Base_Spend_Recommit_vs_Fcst_No_Derate	string,
Savings_pct_Recommit_vs_Fcst_No_Derate	string,
Savings_Recommit_vs_Fcst_No_Derate	string,
x	string,
Base_Spend_Plan_Derate	string,
Savings_pct_Plan_Derate	string,
Savings_Plan_Derate	string,
Base_Spend_Recommit_Derate	string,
Savings_pct_Recommit_Derate	string,
Savings_Recommit_Derate	string,
Base_Spend_Forecast_Derate	string,
Savings_pct_Forecast_Derate	string,
Savings_Forecast_Derate	string,
Base_Spend_Recommit_vs_Fcst_Derate	string,
Savings_pct_Recommit_vs_Fcst_Derate	string,
Savings_Recommit_vs_Fcst_Derate	string,
xx	string,
Base_Spend_P_and_L_Recommit_Derate	string,
Savings_pct_P_and_L_Recommit_Derate	string,
Savings_P_and_L_Recommit_Derate	string,
Base_Spend_P_and_L_Forecast_Derate	string,
Savings_pct_P_and_L_Forecast_Derate	string,
Savings_P_and_L_Forecast_Derate	string,
Base_Spend_P_and_L_Recommit_vs_Fcst_Derate	string,
Savings_pct_P_and_L_Recommit_vs_Fcst_Derate	string,
Savings_P_and_L_Recommit_vs_Fcst_Derate	string,
xxx	string,
Actual_Spend	string,
Actual_Savings_pct	string,
Actual_Savings	string,
Actual_vs_Forecast_Spend_Derate	string,
Actual_vs_Forecast_Savings_pct_Derate	string,
Actual_vs_Forecast_Savings_Derate	string,
Actual_vs_Forecast_Spend_No_Derate	string,
Actual_vs_Forecast_Savings_pct_No_Derate	string,
Actual_vs_Forecast_Savings_No_Derate	string,
Include_flag	string,
Com_Rank	string,
Com_Group_Hadoop	string,
Modified	string,
Modified_By	string,
Ignore	string,
last_refresh_date TIMESTAMP
)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""
  )
TBLPROPERTIES (
    'serialization.null.format' = '',
    'skip.header.line.count' = '1');