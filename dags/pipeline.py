import logging
import requests
import pathlib
import airflow
import datetime as dt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from operators.csv_to_postgres import LoadCsvtoPostgresOperator
from requests import exceptions
from utils.csv_utils import normalize_csv

log = logging.getLogger(__name__)

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "provide_context": True,
}

dag = DAG(
    "nyc_covid_pipeline",
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1,
)

def _fetch_data(outfile):
    pathlib.Path("/tmp/data/raw/").mkdir(parents=True, exist_ok=True)
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    logging.info(f"INFO: Fetching data from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
    except exceptions.MissingSchema:
        print(f"{url} is an invalid URL")
    except exceptions.ConnectionError:
        print(f"Unable to connect to {url}")
    else:
        with open(outfile, "w") as f:
            f.write(response.text)


def _transform_to_csv(infile, outfile):
    pathlib.Path("/tmp/data/stg").mkdir(parents=True, exist_ok=True)
    data = pd.read_json(infile)
    data['date_of_interest'] = data['date_of_interest']\
        .apply(lambda date: date.strip('T00:00:00.000'))
    data.to_csv(outfile, index=False, header=False, sep="\t", encoding="utf-8")
    logging.info(f"INFO: Processed {infile} and moved it to {outfile}")


fetch_data = PythonOperator(
    task_id="fetch_data",
    python_callable=_fetch_data,
    dag=dag,
    op_kwargs={
        "outfile": "/tmp/data/raw/covid_data_{{ ds }}.json"
        }
)

transform_to_csv = PythonOperator(
    task_id="transform_to_csv",
    python_callable=_transform_to_csv,
    dag=dag,
    op_kwargs={
        "infile": "/tmp/data/raw/covid_data_{{ ds }}.json",
        "outfile": "/tmp/data/stg/covid_data_{{ ds }}.csv",
    },
)

normalize_covid_csv = PythonOperator(
        task_id='normalize_covid_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/tmp/data/raw/covid_data_{{ ds }}.csv",
            'target': "/tmp/data/stg/covid_data_{{ ds }}.csv"
        },
        dag=dag,
    )

create_covid_data_table = PostgresOperator(
    task_id="create_table_covid",
    postgres_conn_id="covid_postgres",
    sql="sql/create_table.sql",
    dag=dag,
)

load_csv_to_postgres_dwh = LoadCsvtoPostgresOperator(
    task_id='load_to_covid_data_table',
    postgres_conn_id="covid_postgres",
    table="covid_data",
    file_path="/tmp/data/stg/covid_data_{{ ds }}.csv",
    dag=dag,
)

# fetch_data >> transform_to_csv >> normalize_covid_csv >> create_covid_data_table>> load_csv_to_postgres_dwh
fetch_data >> transform_to_csv >> create_covid_data_table >> load_csv_to_postgres_dwh
