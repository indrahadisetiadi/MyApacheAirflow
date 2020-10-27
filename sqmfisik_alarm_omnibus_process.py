import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from datetime import timedelta
import sys

default_args = {
'owner': 'sqm_app',
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
'sqmfisik_alarm_omnibus_process',
catchup=False,
default_args=default_args,
schedule_interval='*/7 * * * *',
concurrency=4,
max_active_runs=1
# schedule_interval=timedelta(minutes=5),
# schedule_interval= timedelta(days=1)
)


omnibus_proses = "/home/sqm_app/airflow/dags/script/alarm_omnibus/script-omnibus-proses.sh " 


AlarmOmnibusTask = BashOperator(
                task_id='omnibus_proses',
                bash_command=omnibus_proses,
                dag=dag)
                
AlarmOmnibusTask