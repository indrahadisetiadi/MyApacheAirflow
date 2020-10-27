d=$(date --d="-0 day" +"%d")
y=$(date --d="-0 day" +"%Y")
m=$(date --d="-0 day" +"%m")
rfolder=/home/sqm_model/airflow/dags/script/Utilization/data
rhdfs=/data/staging/oss

for i in $(find $rfolder/utilization*.csv | grep ${y}_${m});do
        echo $i
        j=$(echo $i | cut -d'/' -f 9)
        #echo $j
        year=$(echo $j | cut -d'_' -f 2)
        month=$(echo $j | cut -d'_' -f 3)
        day=$(echo $j | cut -d'_' -f 4)
        ds="$year-$month-$day"
        echo $ds
        hdfs dfs -mkdir -p $rhdfs/performance_ehealth_utilization/ds=$ds
        hdfs dfs -moveFromLocal $i $rhdfs/performance_ehealth_utilization/ds=$ds
done

beeline -u "jdbc:hive2://jt-hdp02i0102.telkom.co.id:2181,jt-hdp02i0103.telkom.co.id:2181,jt-hdp02i0407.telkom.co.id:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;ssl=true" -e "MSCK REPAIR TABLE staging.oss_performance_ehealth_utilization;"
