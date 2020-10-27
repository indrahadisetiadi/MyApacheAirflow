ds_now=`date -d "now -0 day" +%Y%m%d`
jam=`date -d "now -0 minutes" +%H`

echo $ds_now
echo $jam

echo Starting on date 

rsync -av --partial --progress sigma@172.24.236.14::sigma/data_hasilukur_api_${ds_now}-${jam}.txt /home/sqm_app/airflow/dags/script/hasil_ukur/api/data/

