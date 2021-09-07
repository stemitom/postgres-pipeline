import json
import requests
import airflow
import datetime as dt
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.operators import csv_to_postgres

from requests import exceptions
from pathlib import Path

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(7),
    "provide_context": True,
}

dag = airflow.DAG(
    "nyc_covid_pipeline",
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1,
)


def _fetch_data(infile):
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    try:
        response = requests.get(url)
    except exceptions.MissingSchema:
        print(f"{url} is an invalid URL")
    except exceptions.ConnectionError:
        print(f"Unable to connect to {url}")
    else:
        with open(infile, "w") as f:
            f.write(response.text)


def _transform_to_csv(infile, outfile):
    content = json.loads(infile)
    data = pd.DataFrame(content)
    data = data.set_index("data_of_interest")
    data.to_csv(outfile)

def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connection_id = kwargs['connection_id']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.bulk_load(table_name, csv_filepath)
    return table_name


fetch_data = PythonOperator(
    task_id="fetch_data",
    python_callable=_fetch_data,
    dag=dag,
    op_kwargs={"infile": "/data/covid_data_{{ds}}.json"},
)

transform_to_csv = PythonOperator(
    task_id="transform_to_csv",
    python_callable=_transform_to_csv,
    dag=dag,
    op_kwargs={
        "infile": "/data/covid_data_{{ds}}.json",
        "outfile": "/data/covid_data_{{ds}}.csv",
    },
)


create_table = PostgresOperator(
    task_id="create_table_covid",
    postgres_conn_id="covid_postgres",
    sql="sql/create_table.sql",
    dag=dag,
)

load_csv_to_postgres = PythonOperator(
    task_id='load_to_covid_data_table',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'csv_filepath': "/data/covid_data_{{ ds }}.csv",
        'table_name': 'covid_data'
    },
    dag=dag
)


fetch_data >> transform_to_csv >> create_table>> load_csv_to_postgres
