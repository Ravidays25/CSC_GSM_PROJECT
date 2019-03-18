SET hive.exec.parallel=true;
set hive.exec.tmp.maprfsvolume=false;
set hive.auto.convert.join=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing=true;
set tez.am.resource.memory.mb=6144;
set hive.exec.orc.split.strategy=BI;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.tez.container.size=10240;

with glp_t as(
SELECT c.quarter_num,
c.period_year,
c.current_qtr_flag,
CONCAT (SUBSTR (c.current_qtr, 0, 4), SUBSTR (c.current_qtr, 7, 2))  AS   fiscal_quarter_name,--Q2FY2019
CONCAT ( CONCAT (SUBSTR (fn.current_qtr, 3, 2), SUBSTR (fn.current_qtr, 7, 2)), SUBSTR (fn.current_qtr, 0, 2)) AS baao_fiscal_period,--Q3FY2019
fn.quarter_start_date   AS fn_quarter_start_date,
fn.quarter_end_date     AS fn_quarter_end_date
FROM csclib.gl_periods_d c,
csclib.gl_periods_d fn
WHERE c.period_set_name = 'Fiscal Year'
AND c.period_num IN (3, 6,  9, 12)
AND c.quarter_num = ${quarter_num}
AND c.period_year = ${period_year}
AND fn.current_qtr = c.first_next_qtr
AND c.period_set_name = fn.period_set_name
AND fn.period_num IN (3, 6, 9, 12)
)

INSERT OVERWRITE TABLE mosdev.mos_spend_and_saving_trend_details PARTITION (baao_fiscal_period,week_num)
select
baao.inventory_item_id ,
baao.item_number,
baao.request_id,
baao.request_line_id,
baao.organization_id,
baao.organization_code,
baao.mpn,
baao.business_unit ,
baao.product_family ,
baao.technology_group ,
baao.request_status,
baao.split_percent,
baao.override_split_percent,
ireq.request_type ,
ireq.launch_setting_quote_type as rfq_type,
ireq.cost_type ,
ires.qual_status,
ires.vendor_id,
comm_cd.control_code,
comm_cd.outsourcing_flag,
comm_cd.alternate_commodity_mgr,
comm_cd.default_commodity_mgr,
comm_cd.comm_code1,
comm_cd.comm_code2,
comm_cd.comm_code3,
comm_cd.comm_code4,
comm_cd.comm_code5,
comm_cd.comm_code6,
comm_cd.comm_code7,
baao.award_status   ,
baao.COMMODITY_GROUP ,
baao.total_projected_spend_cpn as cpn_spend,
baao.total_base_spend_cpn as base_cpn_spend,
baao.total_savings_cpn as cpn_saving,
baao.total_projected_spend as mpn_spend,
baao.total_base_spend_mpn as base_mpn_spend,
baao.total_savings_mpn as mpn_savings,
baao.mpn_split_demand,
baao.demand_quantity as cpn_demand,
sum(total_projected_spend_cpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.award_status) as total_cpn_spend,
--sum(total_base_spend_cpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.award_status) as total_base_cpn_spend,
sum(total_savings_cpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.award_status) as total_cpn_saving,
sum(total_projected_spend) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.mpn,baao.award_status) as total_mpn_spend,
--sum(total_base_spend_mpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.mpn,baao.award_status) as total_base_mpn_spend,
sum(total_savings_mpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.mpn,baao.award_status) as total_mpn_saving,
round((((sum(total_savings_cpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.award_status))/(sum(total_projected_spend_cpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.award_status)))*100),1) as cpn_savings_pct,
round((((sum(total_savings_mpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.mpn,baao.award_status))/(sum(total_base_spend_mpn) over (partition by baao.baao_fiscal_period,baao.item_number,baao.organization_id,baao.mpn,baao.award_status)))*100),1) as mpn_savings_pct,
baao.fiscal_quarter_name,
CURRENT_TIMESTAMP LAST_REFRESH_DATE,
baao.baao_fiscal_period,
baao.week_num
from
(select
inventory_item_id,
item_number,
request_id,
request_line_id,
organization_id,
organization_code ,
request_status ,
award_status   ,
commodity_manager  ,
COMMODITY_GROUP  ,
business_unit ,
product_family ,
technology_group ,
using_requirements_quantity as demand_quantity,
currency  ,
true_cost,
quoted_cost,
awarded_cost,
mpn  ,
response_status  ,
mpn_quoted_cost ,
mpn_true_cost  ,
split_percent  ,
baseline_split_percent ,
override_split_percent,
total_projected_spend,
total_projected_spend_cpn,
total_savings_cpn,
total_base_spend_cpn,
total_base_spend_mpn,
total_savings_mpn,
mpn_split_demand,
pw2y.fiscal_week_in_qtr_num_int as  week_num,
pw2y.fiscal_quarter_name,
hist_snap.baao_fiscal_period as baao_fiscal_period
FROM supplychain_baao.GSM_BAAO_POR_KPI_NEGDT_SNAPSHOT  hist_snap,glp_t oglpd,reference_tdprod_datalakepvwdb.pv_fiscal_week_to_year pw2y
WHERE hist_snap.BAAO_FISCAL_PERIOD = oglpd.baao_fiscal_period
AND hist_snap.AWARD_STATUS <>'CANCELLED'
--`and hist_snap.total_base_spend_cpn<>'NULL'
AND pw2y.fiscal_calendar_code = 'Fiscal Year'
AND CONCAT (
CONCAT (SUBSTR (pw2y.fiscal_quarter_name, 0, 2),SUBSTR (pw2y.fiscal_quarter_name, 4, 2)),SUBSTR (pw2y.fiscal_quarter_name, 8,2))=oglpd.fiscal_quarter_name
--and pw2y.current_fiscal_week_flag='Y'
AND pw2y.fiscal_week_in_qtr_num_int=${week_num}
AND (date_add(pw2y.fiscal_week_start_date,${add_week})>=TO_DATE(hist_snap.VALID_FROM_DATE) AND date_add(pw2y.fiscal_week_start_date,${add_week})<= TO_DATE(hist_snap.VALID_TO_DATE))

) baao
LEFT JOIN

(       SELECT  distinct inventory_item_id ,
                                                item_number ,
                                                request_id ,
                                                organization_id ,
                                                request_type ,
                                                launch_setting_quote_type ,
                                                cost_type,
                                                ire.baao_fiscal_period

       FROM   supplychain_baao.gsm_baao_item_requests ire,
              glp_t glpd
       WHERE  ire.baao_fiscal_period = glpd.baao_fiscal_period
       AND    cost_type ='FIRM'       
       AND    award_status <> 'CANCELLED'
              --and item_number ='74-101624-06'
) ireq
ON baao.item_number = ireq.item_number
AND baao.request_id = ireq.request_id
and baao.item_number = ireq.item_number
and baao.organization_id = ireq.organization_id
and baao.baao_fiscal_period = ireq.baao_fiscal_period

LEFT JOIN

(SELECT
                                                   item_number
                                                   , request_id
                                                   , inventory_item_id
                                                   , request_line_id
                                                   , organization_id
                                                   , qual_status
                                                   , vendor_id
                                                   , updated_flag
                                                   , COST_TYPE
                                                   , RECOMMENDED_SPLIT_PERCENT
                                                   , launch_setting_quote_type
                                                   , irs.baao_fiscal_period

                                       FROM
                                                    supplychain_baao.gsm_baao_item_responses irs
                                                  , glp_t glpd
                                       WHERE
                                                    irs.baao_fiscal_period = glpd.baao_fiscal_period
                                                    AND cost_type            ='FIRM' 
                                                    AND award_status        <> 'CANCELLED'
                                                    --and item_number ='74-101624-06'
                          )
                          ires
                          ON
                            ireq.baao_fiscal_period  = ires.baao_fiscal_period
                            AND baao.item_number     = ires.item_number
                            AND baao.request_id      = ires.request_id
                            AND baao.request_line_id=ires.request_line_id
                            AND baao.organization_id = ires.organization_id
                            AND ireq.COST_TYPE       = ires.COST_TYPE

LEFT JOIN

(             SELECT DISTINCT
                           INVENTORY_ITEM_ID
                         , item_name
                         , organization_id
                         , organization_code
                         , alternate_commodity_mgr
                         , default_commodity_mgr
                         , comm_code1
                         , comm_code2
                         , comm_code3
                         , comm_code4
                         , comm_code5
                         , comm_code6
                         , comm_code7
                         , commodity_risk_rating
                         , commodity_risk_code
                         , item_description
                         , sucm
                         , control_code
                         , outsourcing_flag
                         , outsourced_assembly
                         , comm_group3
              FROM
                           csclib.components_d cmd_p
              WHERE      1=1
                         AND ORGANIZATION_CODE='GLO'
 )
 comm_cd
 ON
              baao.item_number = comm_cd.item_name
;

