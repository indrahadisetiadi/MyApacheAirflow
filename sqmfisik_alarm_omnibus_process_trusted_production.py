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
'sqmfisik_alarm_omnibus_process_trusted_production',
catchup=False,
default_args=default_args,
schedule_interval='15 * * * *',
concurrency=7,
max_active_runs=1
# schedule_interval=timedelta(minutes=5),
# schedule_interval= timedelta(days=1)
)


omnibus_trusted = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/script-trusted.sh "
omnibus_production_akses = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/omnibus_akses.sh "
omnibus_production_broadband = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/omnibus_broadband.sh "
omnibus_production_core = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/omnibus_core.sh "
omnibus_production_cpe = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/omnibus_cpe.sh "
omnibus_production_dwdm = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/omnibus_dwdm.sh "
omnibus_production_metro = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/omnibus_metro.sh "
omnibus_production_voice = "/home/sqm_ingest1/script/alarm_omnibus/trusted_production/omnibus_voice.sh "

AlarmOmnibusTrustedTask = BashOperator(
                task_id='omnibus_trusted_proses',
                bash_command=omnibus_trusted,
                dag=dag)

AlarmOmnibusProductionAccessTask = BashOperator(
                task_id='omnibus_production_akses',
                bash_command=omnibus_production_akses,
                dag=dag)

AlarmOmnibusProductionBroadbandTask = BashOperator(
                task_id='omnibus_production_broadband',
                bash_command=omnibus_production_broadband,
                dag=dag)

AlarmOmnibusProductionCoreTask = BashOperator(
                task_id='omnibus_production_core',
                bash_command=omnibus_production_core,
                dag=dag)

AlarmOmnibusProductionCPETask = BashOperator(
                task_id='omnibus_production_cpe',
                bash_command=omnibus_production_cpe,
                dag=dag)

AlarmOmnibusProductionDWDMTask = BashOperator(
                task_id='omnibus_production_dwdm',
                bash_command=omnibus_production_dwdm,
                dag=dag)

AlarmOmnibusProductionMetroTask = BashOperator(
                task_id='omnibus_production_metro',
                bash_command=omnibus_production_metro,
                dag=dag)

AlarmOmnibusProductionVoiceTask = BashOperator(
                task_id='omnibus_production_voice',
                bash_command=omnibus_production_voice,
                dag=dag)

AlarmOmnibusProductionAccessTask.set_upstream(AlarmOmnibusTrustedTask)
AlarmOmnibusProductionBroadbandTask.set_upstream(AlarmOmnibusTrustedTask)
AlarmOmnibusProductionCoreTask.set_upstream(AlarmOmnibusTrustedTask)
AlarmOmnibusProductionCPETask.set_upstream(AlarmOmnibusTrustedTask)
AlarmOmnibusProductionDWDMTask.set_upstream(AlarmOmnibusTrustedTask)
AlarmOmnibusProductionMetroTask.set_upstream(AlarmOmnibusTrustedTask)
AlarmOmnibusProductionVoiceTask.set_upstream(AlarmOmnibusTrustedTask)