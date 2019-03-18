with glp_t as(
SELECT
c.period_year,
c.quarter_num,
c.current_qtr_flag,
CONCAT (CONCAT(SUBSTR (c.current_qtr, 1, 2), " "), SUBSTR (c.current_qtr, 3, 2),SUBSTR (c.current_qtr, 5, 4))  AS   fiscal_quarter_name,
CONCAT ( CONCAT (SUBSTR (fn.current_qtr, 3, 2), SUBSTR (fn.current_qtr, 7, 2)), SUBSTR (fn.current_qtr, 0, 2)) AS baao_fiscal_period,--FY19Q4
pw2y.fiscal_week_in_qtr_num_int as current_week_num,
pw2y.fiscal_week_in_qtr_num_int-1 as prev_week_num,
pw2y.fiscal_week_in_qtr_num_int-2 as p2_week_num
FROM csclib.gl_periods_d c,
csclib.gl_periods_d fn,
reference_tdprod_datalakepvwdb.pv_fiscal_week_to_year pw2y
WHERE  c.period_set_name = 'Fiscal Year'
AND c.period_num IN (3, 6,  9, 12)
AND c.current_qtr_flag = 'Y'
AND fn.current_qtr = c.first_next_qtr
AND c.period_set_name = fn.period_set_name
AND fn.period_num IN (3, 6, 9, 12)
AND CONCAT (SUBSTR (c.current_qtr, 0, 4), SUBSTR (c.current_qtr, 7, 2))=CONCAT(CONCAT (SUBSTR (pw2y.fiscal_quarter_name, 0, 2),SUBSTR (pw2y.fiscal_quarter_name, 4, 2)),SUBSTR (pw2y.fiscal_quarter_name, 8, 2))
and pw2y.current_fiscal_week_flag='Y'
--and fiscal_week_in_qtr_num_int=1
)

INSERT OVERWRITE TABLE mosdev.GSM_COMMGROUP_WEEKLY_SPEND_FORECAST_F PARTITION (baao_period,fiscal_week_num)
select
glpd.period_year as fiscal_period,
glpd.quarter_num as fiscal_quarter_num,
glpd.current_week_num,
current_date as file_read_date,
commodity,
upper(commodity) AS commodity_group,
"AWARDED" AS award_status,
coalesce(cast(regexp_replace(base_spend_forecast_no_derate,'\\$|\\,','') as bigint),0) as base_spend,
coalesce(cast(regexp_replace(savings_forecast_no_derate,'\\$|\\,','') as bigint),0) as savings,
coalesce(regexp_replace(savings_pct_forecast_no_derate,'\\%',''),0) as savings_percentage,
glpd.fiscal_quarter_name,
'GSM SMARTSHEET (Quarterly Commit vs. Fcst)' as data_source,
CURRENT_TIMESTAMP as last_refresh_date,
glpd.baao_fiscal_period as baao_period,
glpd.prev_week_num as fiscal_week_num
from csclibrary_staging.mos_weekly_forecast_vs_projections_stg,glp_t glpd
where 1=1 
and
Commodity  in ('Software Royalty','Software Digital COGS Infra','TOTAL PRODUCT');
