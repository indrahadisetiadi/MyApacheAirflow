import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from datetime import timedelta

import sys
sys.path.append("/home/840074/airflow/dags/rca/performance_nodeakses/")
from ingest_data_nodeakses import ingestMemory,ingestCPU,ingestTemperature,ingestOccupancy


default_args = {
'owner': '840074',
'start_date': datetime(2020, 6, 12),
'depends_on_past': False,
  #With this set to true, the pipeline won't run if the previous day failed
'email': ['indrahadisetiadi94@gmail.com'],
'email_on_failure': True,
 #upon failure this pipeline will send an email to your email set above
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=2),
'catchup_by_default': False
}

dag = DAG(
'sqmrca_ne_access',
catchup=False,
default_args=default_args,
schedule_interval='05,35 * * * *',
concurrency=4,
max_active_runs=1
# schedule_interval=timedelta(minutes=5),
# schedule_interval= timedelta(days=1)
)

execute_rsync = "/home/840074/airflow/dags/rca/performance_nodeakses/rsync.sh "

rsyncTask = BashOperator(
                task_id='rsync_ne_access_task',
                bash_command=execute_rsync,
                dag=dag)
insert_ne_access_memory_task = PythonOperator(
                task_id='insert_ne_access_memory_task',
                python_callable=ingestMemory,
                dag=dag)

insert_ne_access_cpu_task = PythonOperator(
                task_id='insert_ne_access_cpu_task',
                python_callable=ingestCPU,
                dag=dag)

insert_ne_access_temperature_task = PythonOperator(
                task_id='insert_ne_access_temperature_task',
                python_callable=ingestTemperature,
                dag=dag)

insert_ne_access_occupancy_task = PythonOperator(
                task_id='insert_ne_access_occupancy_task',
                python_callable=ingestOccupancy,
                dag=dag)

rsyncTask >> insert_ne_access_memory_task
rsyncTask >> insert_ne_access_cpu_task
rsyncTask >> insert_ne_access_temperature_task
rsyncTask >> insert_ne_access_occupancy_task
(sqm_env) -bash-4.2$