create table mosdev.GSM_COMMGROUP_SPEND_PROJECTIONS_F
(
fiscal_period int,
fiscal_quarter_num int,
file_read_date timestamp,
commodity string,
commodity_group string,
award_status string,
base_spend double,
savings  double,
savings_percentage double,
fiscal_quarter_name string,
data_source string,
last_refresh_date timestamp)
PARTITIONED BY (baao_period string)
STORED AS parquet
TBLPROPERTIES ('parquet.compression'='GZIP','serialization.null.format' = 'null');
