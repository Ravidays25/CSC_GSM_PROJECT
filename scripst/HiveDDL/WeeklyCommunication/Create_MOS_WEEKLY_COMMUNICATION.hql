create table mosdev.MOS_WEEKLY_COMMUNICATION_with_base_spend
--create table supply_chain_gsm_selfservice.MOS_WEEKLY_COMMUNICATION
(
commodity_group string,
forecast_base_spend double,
forecast_spend double,
forecast_saving double,
forecast_savings_percentage double,
forecast_data_source string,
projection_base_spend double,
projection_savings double,
projection_savings_percentage double,
projection_data_source string,
projection_file_read_date timestamp,
forecast_file_read_date timestamp,
fiscal_quarter_name string,
last_refresh_date timestamp)
PARTITIONED BY (baao_period string,fiscal_week_num int)
STORED AS parquet
TBLPROPERTIES ('parquet.compression'='GZIP','serialization.null.format' = 'null');
