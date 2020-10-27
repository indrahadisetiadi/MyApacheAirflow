rfolder=/mnt/sqm_data_poller/omnibus/csv
rhdfs=/data/staging/oss
d=$(date --d="-0 day" +"%d")
y=$(date --d="-0 day" +"%y")
m=$(date --d="-0 day" +"%m")

#H=$(date --d="-0 day" +"%H")
#if [ $H -gt 20 ] || [ $H -lt 7 ]; then
#   echo "only periodik"
#   bash /home/865433/sqm_production/auto_production/fisik/auto_hasilukur_periodik_predictive.sh
#   for i in $(find /home/865433/sqm_production/auto_production/fisik/hasil_ukur_api/ | grep split_ukur_api);do
#		rm -rf $i	
#	done
#else
#   echo "all script"
#   bash /home/865433/sqm_production/auto_production/fisik/auto_alarm_predictive.sh
#   bash /home/865433/sqm_production/auto_production/fisik/auto_hasilukur_periodik_predictive.sh
#   bash /home/865433/sqm_production/auto_production/fisik/auto_hasilukur_api_predictive.sh
#fi

##matikan sementara trouble koneksi hadoop 26072020
for i in $(find $rfolder/omnibus_output_*-*_insert_prod.csv | grep $y$m$d | grep -v "PROCESS");do
        echo $i
        j=$(echo $i | cut -d'/' -f 6)
        j=$(echo $j | cut -d'_' -f 3)
        year=$(echo $j | cut -c 1-2)
        month=$(echo $j | cut -c 3-4)
        day=$(echo $j | cut -c 5-6)
        ds="20$year-$month-$day"
        echo $ds
        hdfs dfs -mkdir -p $rhdfs/omnibus_insert/ds=$ds
        echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -mkdir -p $rhdfs/omnibus_insert/ds=$ds"
        hadoop fs -moveFromLocal $i $rhdfs/omnibus_insert/ds=$ds
        echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -put $i $rhdfs/omnibus_insert/ds=$ds"
	rm -rf $i
done

bash /home/sqm_app/airflow/dags/script/alarm_omnibus/magic.sh
