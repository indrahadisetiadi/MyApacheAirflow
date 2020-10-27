d=$(date --d="-0 day" +"%d")
y=$(date --d="-0 day" +"%Y")
m=$(date --d="-0 day" +"%m")
rfolder=/home/sqm_app/airflow/dags/script/hasil_ukur/api
rhdfs=/data/staging/oss
for i in $(find $rfolder/data/data_hasilukur_api*.txt | grep $y$m$d);do
        echo $i
        j=$(echo $i | cut -d'/' -f 10)
        #echo $j
        j=$(echo $j | cut -d'_' -f 4)
        year=$(echo $j | cut -c 1-4)
        month=$(echo $j | cut -c 5-6)
        day=$(echo $j | cut -c 7-8)
        ds="$year-$month-$day"
        echo $ds
        hdfs dfs -mkdir -p $rhdfs/performance_individu_hasilukur_api/ds=$ds
        hdfs dfs -moveFromLocal $i $rhdfs/performance_individu_hasilukur_api/ds=$ds
done
