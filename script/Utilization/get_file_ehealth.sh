i=0
pfolder=/mnt/sqm_data_poller/ehealth
tgl=`date -d "now - 1 minutes" '+%Y%m%d_%H_%M'`
from=`date -d "now - 10 minutes" '+%Y-%m-%d %H:%M:00'`
to=`date -d "now" '+%Y-%m-%d %H:%M:00'`
echo $from to $to
rfolder=/home/sqm_model/airflow/dags/script/Utilization
> $rfolder/data/ehealth_raw.csv
files=`find $pfolder/ -maxdepth 1 -newermt "${from}" ! -newermt "$to" | grep polledRateData`
for filename in $files; do
  echo $filename
  if [[ $i -eq 0 ]] ; then
    # copy csv headers from first file
    echo "first file"
    head -1 $filename > $rfolder/data/ehealth_raw.csv
  fi
  # copy csv without headers from other files
    tail -n +2 $filename | grep NormalizedPortInfo >> $rfolder/data/ehealth_raw.csv
   
   i=$(( $i + 1 ))
done
cat $rfolder/data/ehealth_raw.csv | grep Utilization > $rfolder/data/raw_ehealth_clean.csv
