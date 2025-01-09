import sys

sys.path.insert(0, "/opt/airflow/dags")

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from connection import create_engine
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def _fetch_data(task_instance: TaskInstance | None = None):
    import requests
    import polars as pl

    response = requests.get(f'https://hari-libur-api.vercel.app/api?year={task_instance.execution_date.year}')
    data = pl.DataFrame(response.json()).with_columns(
        pl.col('event_date').map_elements(lambda x: x.replace('-', ''), return_dtype=pl.String),
        pl.col('is_national_holiday').map_elements(lambda x: "Holiday" if x else "Non-Holiday", return_dtype=pl.String),
    )
    # Create sqlsalchemy engine to connect the postgres db
    engine = create_engine(
        cred_variable="db_credentials", database="warehouse"
    )
    data.write_database('staging.stg_events', engine, if_table_exists='replace')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "monthly_dim_date_update",
    default_args=default_args,
    description="Monthly fetch national event or holiday data.",
    schedule_interval="0 0 1 * *",
    start_date=datetime(2022, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["general", "date"],
    template_searchpath="/opt/airflow/dags/sql",
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=_fetch_data,
        execution_timeout=timedelta(minutes=15),
    )

    upsert_data = SQLExecuteQueryOperator(
        task_id='event_upsert',
        sql="dim_date_event_update.sql",
        conn_id="postgres-dw"
    )
    
    fetch_data >> upsert_data
