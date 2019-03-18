#!/bin/bash -x

echo "Spend And Saving Script Started"
echo "Input values are , p1_week_num p2_week_num p1_quarter_num p1_period_year p2_quarter_num  p2_period_year : : "$1 $2 $3 $4 $5 $6
job="/users/hdpcscgsm/ranaraha/Weekly_logic/Populate_MOS_SPEND_AND_SAVING_TREND_DETAILS_with_base_spend"
echo " Loop Started..."
for i in {1..2}
	do
	if ( [ $1 ==  12 ] || [ $2 == 12 ] ) ; then
		add_week=4
	else
		add_week=6
	fi	
		
	if [ $i == 1 ] ; then
           hive --hiveconf tez.am.resource.memory.mb=10240 --hivevar quarter_num=${3} --hivevar period_year=${4} --hivevar week_num=${1} --hivevar add_week=${add_week} -f ${job}.hql > ${job}_p1.log 2>&1 &
	elif [ $i == 2 ] ; then
          hive --hiveconf tez.am.resource.memory.mb=10240 --hivevar quarter_num=${5} --hivevar period_year=${6} --hivevar week_num=${2} --hivevar add_week=${add_week} -f ${job}.hql > ${job}_p2.log 2>&1 &
 
	fi
done
echo "Loop End."
sleep 60
grep 'App id application_' ${job}*.log
wait;
echo "Spend And Saving Script Completed"
