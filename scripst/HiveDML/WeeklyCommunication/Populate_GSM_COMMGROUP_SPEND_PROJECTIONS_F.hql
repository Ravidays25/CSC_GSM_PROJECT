with glp_t as(
SELECT
c.period_year,
c.quarter_num,
c.current_qtr_flag,
CONCAT (CONCAT(SUBSTR (c.current_qtr, 1, 2), " "), SUBSTR (c.current_qtr, 3, 2),SUBSTR (c.current_qtr, 5, 4))  AS   fiscal_quarter_name,
CONCAT ( CONCAT (SUBSTR (fn.current_qtr, 3, 2), SUBSTR (fn.current_qtr, 7, 2)), SUBSTR (fn.current_qtr, 0, 2)) AS baao_fiscal_period--FY19Q4
FROM csclib.gl_periods_d c,
csclib.gl_periods_d fn
WHERE  c.period_set_name = 'Fiscal Year'
AND c.period_num IN (3, 6,  9, 12)
AND c.current_qtr_flag = 'Y'
AND fn.current_qtr = c.first_next_qtr
AND c.period_set_name = fn.period_set_name
AND fn.period_num IN (3, 6, 9, 12)
)



INSERT OVERWRITE TABLE mosdev.GSM_COMMGROUP_SPEND_PROJECTIONS_F PARTITION (baao_period)
select 
fiscal_period,
fiscal_quarter_num,
file_read_date,
commodity,
CASE
WHEN commodity_group='MS&T' THEN 'MIXED SIGNAL & TIMING'
WHEN commodity_group='PD&L' THEN 'PASSIVE DISCRETE & LOGIC'
WHEN commodity_group='INTERCONNECT' THEN 'INTERCONNECTS'
WHEN commodity_group='SPECIALTY TECHNOLOGY' THEN 'SPECIALTY TECH'
WHEN commodity_group IS NULL THEN 'UNASSIGNED'
ELSE commodity_group
END AS commodity_group,
"AWARDED" AS award_status,
base_spend,
savings,
savings_percentage,
fiscal_quarter_name,
data_source,
last_refresh_date,
baao_period
from(select
glpd.period_year as fiscal_period,
glpd.quarter_num as fiscal_quarter_num,
--glpd.current_qtr_flag,
current_date as file_read_date,
commodity,
upper(commodity) AS commodity_group,
coalesce(cast(regexp_replace(base_spend_recommit_no_derate,'\\$|\\,','') as bigint),0) as base_spend,
coalesce(cast(regexp_replace(savings_recommit_no_derate,'\\$|\\,','') as bigint),0) as savings,
coalesce(regexp_replace(savings_pct_recommit_no_derate,'\\%',''),0) as savings_percentage,
glpd.fiscal_quarter_name,
'GSM SMARTSHEET (Quarterly Commit vs. Fcst)' as data_source,
CURRENT_TIMESTAMP as last_refresh_date,
glpd.baao_fiscal_period as baao_period
from csclibrary_staging.mos_weekly_forecast_vs_projections_stg,glp_t glpd
where 1=1
-- and Commodity RLIKE '^[A-za-z&/\,() ]*$'
and Commodity  in
('ASICS','Communications','E/M Other','Enclosures','HDD','Interconnect','Memory','MS&T','MPU','OEM/ODM','Optics'
,'PD&L','PCB','PLD','Power','Software Digital COGS Infra','Software Royalty','Specialty Technology','Thermal','TOTAL PRODUCT')
)dc;
