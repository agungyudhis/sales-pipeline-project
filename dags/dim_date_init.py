import sys

sys.path.insert(0, "/opt/airflow/dags")
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.mysql_operator import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from connection import create_engine


def init_dim_date_table():
    from datetime import date

    import polars as pl

    START_YEAR = 2020
    END_YEAR = 2030

    # Create sqlsalchemy engine to connect the postgres db
    engine = create_engine(cred_variable="db_credentials", database="warehouse")

    date_dimension = (
        pl.date_range(
            date(START_YEAR, 1, 1),
            date(END_YEAR, 1, 1),
            "1d",
            closed="left",
            eager=True,
        )
        .alias("date_temp")
        .to_frame()
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.strftime(r"%Y%m%d").cast(pl.Int32)).alias("date_id")
    )
    date_dimension = date_dimension.with_columns((pl.col("date_temp")).alias("date"))
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.strftime(r"%A, %-d %B %Y")).alias(
            "full_date_description"
        )
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.year()).alias("year")
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.quarter()).alias("quarter")
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.month()).alias("month")
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.day()).alias("day")
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.strftime(r"%u").cast(pl.Int32)).alias("day_of_week")
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.strftime(r"%A")).alias("calendar_day_of_week")
    )
    date_dimension = date_dimension.with_columns(
        (pl.col("date_temp").dt.strftime(r"%B")).alias("calendar_month")
    )
    date_dimension = date_dimension.with_columns(
        pl.col("day_of_week")
        .map_elements(
            lambda x: "Weekend" if x in [6, 7] else "Weekday", return_dtype=pl.String
        )
        .alias("is_weekend")
    )
    date_dimension = date_dimension.drop(["date_temp"])

    with engine.connect() as connection:
        date_dimension.write_database(
            table_name="staging.stg_date",
            connection=connection,
            if_table_exists="replace",
        )
    return None


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "initial_dim_date",
    default_args=default_args,
    description="Create calendar date dimension.",
    schedule_interval="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["initial", "dimension"],
    template_searchpath="/opt/airflow/dags/sql",
) as dag:
    stg_init_dim_date = PythonOperator(
        task_id="stg_init_dim_date",
        python_callable=init_dim_date_table,
        execution_timeout=timedelta(minutes=5),
    )

    transform_stg_dim_date = SQLExecuteQueryOperator(
        task_id="mysql_operator",
        sql="dim_date_init.sql",
        mysql_conn_id="db_credentials",
    )

    stg_init_dim_date >> transform_stg_dim_date
