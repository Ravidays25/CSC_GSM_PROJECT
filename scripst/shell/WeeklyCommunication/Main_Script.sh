#!/bin/bash -x

echo "Main Scripst Started :: " `date +%Y-%m-%d_%H-%M-%S`
results=`hive -e "SELECT 
c.period_year,
c.quarter_num,
c.current_qtr_flag,
CONCAT (SUBSTR (c.current_qtr, 0, 4), SUBSTR (c.current_qtr, 7, 2))  AS   fiscal_quarter_name,--Q3FY2019
CONCAT ( CONCAT (SUBSTR (fn.current_qtr, 3, 2), SUBSTR (fn.current_qtr, 7, 2)), SUBSTR (fn.current_qtr, 0, 2)) AS baao_fiscal_period,--FY19Q4
pw2y.fiscal_week_in_qtr_num_int as current_week_num,
pw2y.fiscal_week_in_qtr_num_int-1 as p1_week_num,
pw2y.fiscal_week_in_qtr_num_int-2 as p2_week_num
FROM csclib.gl_periods_d c,
csclib.gl_periods_d fn,
reference_tdprod_datalakepvwdb.pv_fiscal_week_to_year pw2y
WHERE  c.period_set_name = 'Fiscal Year'
AND c.period_num IN (3, 6,  9, 12)
--AND c.current_qtr_flag = 'Y'
AND c.quarter_num =3
AND c.period_year = 2019
AND fn.current_qtr = c.first_next_qtr
AND c.period_set_name = fn.period_set_name
AND fn.period_num IN (3, 6, 9, 12)
AND CONCAT (SUBSTR (c.current_qtr, 0, 4), SUBSTR (c.current_qtr, 7, 2))=CONCAT(CONCAT (SUBSTR (pw2y.fiscal_quarter_name, 0, 2),SUBSTR (pw2y.fiscal_quarter_name, 4, 2)),SUBSTR (pw2y.fiscal_quarter_name, 8, 2))
--and pw2y.current_fiscal_week_flag='Y'
and pw2y.fiscal_week_in_qtr_num_int=2
"`;

echo "Result is :: " $results
array=($results)
  
#out put is = 2019 3 Q3FY19 FY19Q4 5 4 3
period_year=${array[0]}
quarter_num=${array[1]}
current_qtr_flag=${array[2]}
fiscal_quarter_name=${array[3]}
baao_fiscal_period=${array[4]}
current_week_num=${array[5]}
p1_week_num=${array[6]}
p2_week_num=${array[7]}
if [ $quarter_num -eq 1 ] ; then # $quarter_num = 1
	echo -e "Current quater falg is  :: ${current_qtr_flag} \nFiscal Quarter is ::${quarter_num} \nFiscal Year is :: ${period_year} \nBaao fiscal period is  :: ${baao_fiscal_period} \ncurrent baao fiscal week number :: ${current_week_num}"
	
	if [ $current_week_num -eq 1 ] ; then 
		p1_quarter_num=4
		p1_period_year=$((period_year-1))
		p2_quarter_num=4
		p2_period_year=$((period_year-1))
		
		echo -e "prev1 Fiscal Quarter is :: ${p1_quarter_num} \nFiscal Year is :: ${p1_period_year}"
		echo -e "prev2 Fiscal Quarter is :: ${p2_quarter_num} \nFiscal Year is :: ${p2_period_year}"
		
	elif [ $current_week_num -eq 2 ] ; then 
		p1_quarter_num=$quarter_num
		p1_period_year=$period_year
		p2_quarter_num=4
		p2_period_year=$((period_year-1))
		echo -e "prev1 Fiscal Quarter is :: ${p1_quarter_num} \nFiscal Year is :: ${p1_period_year}"
		echo -e "prev2 Fiscal Quarter is :: ${p2_quarter_num} \nFiscal Year is :: ${p2_period_year}"
	fi
	
else
	echo -e "Current quater falg is  :: ${current_qtr_flag} \nFiscal Quarter is ::${quarter_num} \nFiscal Year is :: ${period_year} \nBaao fiscal period is  :: ${baao_fiscal_period} \ncurrent baao fiscal week number :: ${current_week_num}"
		
		if [ $current_week_num -eq 1 ] ; then 
		    p1_quarter_num=$((quarter_num-1))
		    p1_period_year=$period_year
		    p2_quarter_num=$((quarter_num-1))
		    p2_period_year=$period_year
		    
		    echo -e "prev1 Fiscal Quarter is :: ${p1_quarter_num} \nFiscal Year is :: ${p1_period_year}"
		    echo -e "prev2 Fiscal Quarter is :: ${p2_quarter_num} \nFiscal Year is :: ${p2_period_year}"
		elif [ $current_week_num -eq 2 ] ; then
		    p1_quarter_num=$quarter_num
		    p1_period_year=$period_year
		    p2_quarter_num=$((quarter_num-1))
		    p2_period_year=$period_year
		    echo -e "prev1 Fiscal Quarter is :: ${p1_quarter_num} \nFiscal Year is :: ${p1_period_year}"
		    echo -e "prev2 Fiscal Quarter is :: ${p2_quarter_num} \nFiscal Year is :: ${p2_period_year}"
		else
	       	    p1_quarter_num=$quarter_num
                    p1_period_year=$period_year
                    p2_quarter_num=$quarter_num
                    p2_period_year=$period_year
		    echo -e "prev1 Fiscal Quarter is :: ${p1_quarter_num} \nFiscal Year is :: ${p1_period_year}"
                    echo -e "prev2 Fiscal Quarter is :: ${p2_quarter_num} \nFiscal Year is :: ${p2_period_year}"

		fi

	
fi

if [ ${current_week_num} == 1 ] ; then 
	p1_week_num=0
	p2_week_num=12

elif [ ${current_week_num} == 2 ] ; then
	p1_week_num=$((current_week_num-1))
	p2_week_num=0
else
	p1_week_num=$((current_week_num-1))
	p2_week_num=$((current_week_num-2))
fi

echo "Input Param values from Main scripst :: " $p1_week_num $p2_week_num $p1_quarter_num $p1_period_year $p2_quarter_num $p2_period_year

#sh Projection_Load_Data.sh $p1_week_num $p2_week_num $p1_quarter_num $p1_period_year $p2_quarter_num $p2_period_year

#sh spend_and_saving_automation.sh $p1_week_num $p2_week_num $p1_quarter_num $p1_period_year $p2_quarter_num $p2_period_year

#sh weekly_communication_automation.sh $p1_week_num $p2_week_num $p1_quarter_num $p1_period_year $p2_quarter_num $p2_period_year


sh spend_and_saving_automation_with_base_spend.sh $p1_week_num $p2_week_num $p1_quarter_num $p1_period_year $p2_quarter_num $p2_period_year

sh weekly_communication_automation_with_base_spend.sh $p1_week_num $p2_week_num $p1_quarter_num $p1_period_year $p2_quarter_num $p2_period_year



echo "Main Scripst Completed :: " `date +%Y-%m-%d_%H-%M-%S`

