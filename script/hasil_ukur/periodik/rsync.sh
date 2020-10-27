ds_now=`date -d "now -0 day" +%Y%m%d`
jam=`date -d "now -0 minutes" +%H`

echo $ds_now
echo $jam

rfolder=/home/sqm_app/airflow/dags/script/hasil_ukur/periodik/data
rscript=/home/sqm_app/airflow/dags/script/hasil_ukur/periodik
status=0
angka=1
fRsync()
{
        rsync -av --partial --progress sigma@172.24.236.14::sigma/backup_hasilukur_periodik_${ds_now}-${jam}.csv $rfolder/
	chmod -R 777 /home/sqm_app/airflow/dags/script/hasil_ukur/periodik/data/
        count=`ls $rfolder/backup_hasilukur_periodik_${ds_now}-${jam}.csv | wc -l`
        echo "percobaan rsync ke - ${angka}"
        if [ $count -gt 0 ]
        then
                status=1
                echo "data ada setetelah di rsync"
        fi
}
data=`ls $rfolder/backup_hasilukur_periodik_${ds_now}-${jam}.csv`
if [ -z "$data" ]
then
        while [ $status -eq 0 ];
        do
                fRsync
                if [ $angka -gt 4 ]
                then
                        echo "data ga ada terus setelah 5 menit nyobain rsync"
                        break
                fi
                if [ $status -gt 0 ]
                then
                        echo "jalankan script proses"
                        bash $rscript/process-core.sh
                        break
                fi
                echo 'sleep 1m'
                sleep 1m
                angka=$((angka+1))
        done
else
        echo "jalankan script proses"
        bash $rscript/process-core.sh
fi

