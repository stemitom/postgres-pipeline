import airflow

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="joke",
    start_date=days_ago(2)
)
dummy_one = DummyOperator(
    task_id="test_dag_1",
    dag=dag
)

dummy_two = DummyOperator(
    task_id="test_dag_2",
    dag=dag
)

dummy_one >> dummy_two