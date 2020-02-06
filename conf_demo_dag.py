import logging
from datetime import timedelta, datetime
from functools import partial
import os

from airflow import DAG

from demo_operators import DemoPythonOperator, DemoQuboleOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime.strptime('2020-02-05T00:00:00', '%Y-%m-%dT%H:%M:%S'),
    'email': ['admin@qubole.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'queue': 'default'
}


dag = DAG(
        'bifrost_curator',
        default_args=default_args,
        max_active_runs=1,
        schedule_interval='@hourly'
      )



# DAG definition starts here

task1 = DemoPythonOperator("task1", run=task1_exec)
task1(dag)


task2 = DemoPythonOperator("task2", run=task2_exec)
task1(dag)

task2.task.set_upstream(task1)
