import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from datetime import timedelta
import sys
sys.path.append("/home/sqm_model/airflow/dags/script/Utilization/")
from cleansing_utilization import cleansing


default_args = {
'owner': 'sqm_model',
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
'sqmlogic_ehealth_utilization',
catchup=False,
default_args=default_args,
schedule_interval='*/10 * * * *',
concurrency=4,
max_active_runs=1
# schedule_interval=timedelta(minutes=5),
# schedule_interval= timedelta(days=1)
)


get_data_ehealth = "/home/sqm_model/airflow/dags/script/Utilization/get_file_ehealth.sh "
move_to_staging = "/home/sqm_model/airflow/dags/script/Utilization/move-to-staging.sh "

getDataTask = BashOperator(
                task_id='get_data_ehealth_task',
                bash_command=get_data_ehealth,
                dag=dag)

cleansing_ehealth_task = PythonOperator(
                task_id='cleansing_ehealth_task',
                python_callable=cleansing,
                dag=dag)

move_to_staging_task = BashOperator(
                task_id='move_to_staging_task',
                bash_command=move_to_staging,
                dag=dag)

getDataTask >> cleansing_ehealth_task >> move_to_staging_task