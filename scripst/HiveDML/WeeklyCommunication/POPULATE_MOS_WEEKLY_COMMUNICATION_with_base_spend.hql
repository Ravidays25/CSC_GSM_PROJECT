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
--AND c.current_qtr_flag = 'Y'
--AND c.quarter_num = 4
--AND c.period_year = 2018
AND fn.current_qtr = c.first_next_qtr
AND c.period_set_name = fn.period_set_name
AND fn.period_num IN (3, 6, 9, 12)
)

--INSERT OVERWRITE TABLE mosdev.MOS_WEEKLY_COMMUNICATION PARTITION (baao_period,fiscal_week_num)
INSERT OVERWRITE TABLE mosdev.mos_weekly_communication_with_base_spend PARTITION (baao_period,fiscal_week_num)
select 
forecast.commodity_group ,
coalesce(forecast.total_base_cpn_spend,0) as forecast_base_spend,
coalesce(forecast.total_cpn_spend,0) as forecast_spend,
coalesce(forecast.total_cpn_saving,0) as forecast_saving,
coalesce(forecast.total_cpn_savings_pct,0) as forecast_savings_percentage,
forecast.data_source as forecast_data_source,
coalesce(projection.base_spend,0) as projection_base_spend,
coalesce(projection.savings,0) as projection_savings,
coalesce(projection.savings_percentage,0) as projection_savings_percentage,
projection.data_source as projection_data_source,
projection.file_read_date as projection_file_read_date,
forecast.file_read_date as forecast_file_read_date,
forecast.fiscal_quarter_name,
CURRENT_TIMESTAMP as last_refresh_date,
forecast.baao_fiscal_period as baao_period,
forecast.week_num as fiscal_week_num
from
(select
sp.commodity_group,sp.award_status,
sum(sp.total_base_cpn_spend) as total_base_cpn_spend,
sum(sp.total_cpn_spend) as total_cpn_spend,
sum(sp.total_cpn_saving) as total_cpn_saving ,
round((sum(sp.total_cpn_saving)/sum(sp.total_cpn_spend))*100,2) as total_cpn_savings_pct,
fiscal_quarter_name,
"POR" as data_source,
sp.last_refresh_date as file_read_date,
--CURRENT_TIMESTAMP AS LAST_REFRESH_DATE,
sp.baao_fiscal_period,
sp.week_num
from
(select distinct spe.baao_fiscal_period,spe.fiscal_quarter_name,item_number,organization_id,award_status,commodity_group,last_refresh_date,
coalesce(total_base_cpn_spend,0) as total_base_cpn_spend ,
coalesce(total_cpn_spend,0) as total_cpn_spend ,
coalesce(total_cpn_saving,0) as total_cpn_saving ,
coalesce(cpn_savings_pct,0) as cpn_savings_pct,
spe.week_num
from mosdev.mos_spend_and_saving_trend_details_with_base_spend spe, glp_t glpd
where spe.baao_fiscal_period=glpd.baao_fiscal_period
and spe.award_status='AWARDED'
--and week_num=6
and week_num=${week_num}
) sp
group by  baao_fiscal_period,fiscal_quarter_name,commodity_group,award_status,week_num,last_refresh_date
UNION ALL
SELECT
fc.commodity_group,
award_status,
base_spend AS total_base_cpn_spend,
null as total_cpn_spend,
savings AS Total_cpn_saving,
savings_percentage AS total_cpn_savings_pct,
fc.fiscal_quarter_name ,
fc.data_source,
fc.file_read_date,
--fc.last_refresh_date,
baao_period AS baao_fiscal_period,
fiscal_week_num AS week_num
FROM mosdev.GSM_COMMGROUP_WEEKLY_SPEND_FORECAST_F fc,glp_t glpd
where fc.baao_period=glpd.baao_fiscal_period
--and fc.award_status='AWARDED'
--and fc.fiscal_week_num=6
and fc.fiscal_week_num=${week_num}
)forecast 
LEFT JOIN
mosdev.gsm_commgroup_spend_projections_f projection
ON forecast.commodity_group=projection.commodity_group
AND projection.baao_period=forecast.baao_fiscal_period;


