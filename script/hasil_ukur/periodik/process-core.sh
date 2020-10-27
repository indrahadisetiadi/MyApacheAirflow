rfolder=/home/sqm_app/airflow/dags/script/hasil_ukur/periodik
rhdfs=/data/staging/oss
#echo "======Performance Node Akses======" > infoTelegram.txt

#PRODUCTION
#bash /home/865433/sqm_production/auto_production/fisik/hasil_ukur_periodik/script-split.sh

for i in $(find $rfolder/data/backup_*.csv);do
#       echo $i
        j=$(echo $i | cut -d'/' -f 10)
        j=$(echo $j | cut -d'_' -f 4)
        year=$(echo $j | cut -c 1-4)
        month=$(echo $j | cut -c 5-6)
        day=$(echo $j | cut -c 7-8)
        ds="$year-$month-$day"
        hour=$(echo $j | cut -c 10-11)
        echo $ds
        echo $hour
        hdfs dfs -mkdir -p $rhdfs/performance_individu_hasilukur_periodik/ds=$ds
        echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -mkdir -p $rhdfs/performance_individu_hasilukur_periodik/ds=$ds"
        hdfs dfs -moveFromLocal $i $rhdfs/performance_individu_hasilukur_periodik/ds=$ds
        echo "`date +%Y-%m-%dT%H:%M:%S` hadoop fs -put $i $rhdfs/performance_individu_hasilukur_periodik/ds=$ds"
done

beeline -u "jdbc:hive2://jt-hdp02i0102.telkom.co.id:2181,jt-hdp02i0103.telkom.co.id:2181,jt-hdp02i0407.telkom.co.id:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;ssl=true" -e "msck repair table staging.oss_performance_individu_hasilukur_periodik;"
