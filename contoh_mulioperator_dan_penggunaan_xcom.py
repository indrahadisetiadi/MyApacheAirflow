import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
from datetime import timedelta

default_args = {
'owner': '840074',
'start_date': airflow.utils.dates.days_ago(0),
'depends_on_past': True,
  #With this set to true, the pipeline won't run if the previous day failed
'email': ['info@example.com'],
'email_on_failure': True,
 #upon failure this pipeline will send an email to your email set above
'email_on_retry': False,
'retries': 5,
'retry_delay': timedelta(minutes=30),
 'catchup_by_default': False,
}

dag = DAG(
'simple_pipeline',
catchup=False,
default_args=default_args,
schedule_interval='*/5 * * * *',
max_active_runs=3,
# schedule_interval=timedelta(minutes=5),
# schedule_interval= timedelta(days=1)
)


def my_func():
    print('Hello from my_func')


test_bash = """
export test_val=`date`
echo $test_val
"""

bash_task = BashOperator(
            task_id='test',
            bash_command=test_bash,
            xcom_push=True,
            retries=3,
            dag=dag)

dummy_task      = BashOperator(
                task_id='dummy_task',
                bash_command="echo sekarang tanggal {{ ti.xcom_pull(task_ids='test') }}",
                retries=3,
                dag=dag)

python_task     = PythonOperator(
                task_id='python_task',
                python_callable=my_func,
                dag=dag)

#python_task dependencies ke bashtask,
#python_task akan di execute jika bashtask success
bash_task.set_downstream(python_task)

# sama seperti diatas, dummy_task akan di execute jika bashtask success
dummy_task.set_upstream(bash_task)