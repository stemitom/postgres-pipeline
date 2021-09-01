import json
import requests
import airflow
import datetime as dt
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from requests import exceptions
from pathlib import Path

args = {
    "owner":"airflow",
    "start_date":airflow.utils.dates.days_ago(7),
    "provide_context":True
}

dag = airflow.DAG(
    "nyc_covid_pipeline",
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1
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
        with open(infile, 'w') as f:
        f.write(response.text)

def _transform_to_csv(infile, outfile):
    content = json.loads(infile)
    data = pd.DataFrame(content)
    data = data.set_index("data_of_interest")
    data.to_csv(outfile)

fetch_data = PythonOperator(
    task_id="fetch_data",
    python_callable=_fetch_data,
    dag=dag,
    op_kwargs={
        "infile":"/data/covid_data_{{ds}}.json"
    }
)

transform_to_csv = PythonOperator(
    task_id="transform_to_csv",
    python_callable=_transform_to_csv,
    dag=dag,
    op_kwargs={
        "infile": "/data/covid_data_{{ds}}.json",
        "outfile": "/data/covid_data_{{ds}}.csv"
    }
)


create_table = PostgresOperator(
    task_id="create_table_covid",
    postgres_conn_id="covid_postgres",
    sql="sql/create_table.sql",
    dag=dag
)

# populate_table = PostgresOperator(
#     task_id="populate_table_covid",
#     postgres_conn_id="covid_postgres"
# )


fetch_data >> transform_to_csv >> create_table