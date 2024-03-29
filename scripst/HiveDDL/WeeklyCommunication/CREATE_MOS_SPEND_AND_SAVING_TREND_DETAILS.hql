CREATE TABLE mosdev.MOS_SPEND_AND_SAVING_TREND_DETAILS_with_base_spend
(
inventory_item_id  string,
item_number string,
request_id int,
request_line_id int,
organization_id int,
organization_code string,
mpn string,
business_unit string,
product_family string,
technology_group string,
request_status string ,
split_percent double,
override_split_percent double,
request_type string ,
rfq_type string ,
cost_type string ,
qual_status string,
vendor_id int,
control_code string,
outsourcing_flag string,
alternate_commodity_mgr string,
default_commodity_mgr string,
comm_code1 string,
comm_code2 string,
comm_code3 string,
comm_code4 string,
comm_code5 string,
comm_code6 string,
comm_code7 string,
award_status string  ,
COMMODITY_GROUP string  ,
cpn_spend double,
base_cpn_spend double,
cpn_saving double,
mpn_spend double,
base_mpn_spend double,
mpn_savings double,
mpn_split_demand double,
cpn_demand double,
total_cpn_spend  double,
total_base_cpn_spend double,
total_cpn_saving double,
total_mpn_spend double,
total_base_mpn_spend double,
total_mpn_saving double,
cpn_savings_pct double,
mpn_savings_pct double,
fiscal_quarter_name string,
last_refresh_date timestamp
)PARTITIONED BY (baao_fiscal_period string,week_num int)
STORED AS parquet
TBLPROPERTIES ('parquet.compression'='GZIP','serialization.null.format' = 'null');
