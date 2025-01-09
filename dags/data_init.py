import sys

sys.path.insert(0, "/opt/airflow/dags")

from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from connection import create_engine


def customer_generator(n_samples: int) -> list:
    from faker import Faker
    from faker.providers import geo

    fake = Faker()
    fake.add_provider(geo)

    customer_list = []
    for cust_id in range(n_samples):
        name = fake.name()
        lat, long, place, country_code, _ = fake.location_on_land()
        customer_list.append(
            {
                "id": f"ID-{str(cust_id).zfill(8)}",
                "name": name,
                "latitude": lat,
                "longitude": long,
                "place": place,
                "country": country_code,
            }
        )
    return customer_list


def product_generator() -> pl.DataFrame:
    product_name = {
        "Product A": 14.99,
        "Product B": 19.99,
        "Product C": 24.99,
        "Product D": 26.99,
    }
    sizes = {"S": -2.0, "M": -1.0, "L": 1.0, "XL": 2.0}
    colors = ["Black", "White", "Gray", "Pink"]

    product = []
    sku_id = 0
    for product_id, (product_name, base_price) in enumerate(product_name.items()):
        for size, additional_price in sizes.items():
            for color in colors:
                product.append(
                    {
                        "sku_id": f"SKU-{str(sku_id).zfill(4)}",
                        "product_id": f"P-{str(product_id).zfill(4)}",
                        "product_name": product_name,
                        "size": size,
                        "color": color,
                        "price": round(base_price + additional_price, 2),
                        "cogs": round(base_price * 0.6, 2),
                    }
                )
                sku_id += 1
    return pl.DataFrame(product)


def _dim_date_init():
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
            table_name="staging.stg_dim_date",
            connection=connection,
            if_table_exists="replace",
        )
    return None

def _customer_init():
    import polars as pl

    data = pl.DataFrame(customer_generator(50000))

    # Create sqlsalchemy engine to connect the postgres db
    engine = create_engine(cred_variable="db_credentials", database="warehouse")

    with engine.connect() as connection:
        data.write_database("synthetic.customers", connection, if_table_exists="fail")


def _product_init():
    import polars as pl

    data = pl.DataFrame(product_generator())

    # Create sqlsalchemy engine to connect the postgres db
    engine = create_engine(cred_variable="db_credentials", database="warehouse")

    with engine.connect() as connection:
        data.write_database("synthetic.products", connection, if_table_exists="fail")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    "data_init",
    default_args=default_args,
    description="Database and synthetic data initialization",
    schedule_interval="@once",
    start_date=datetime(2024, 12, 25),
    catchup=True,
    max_active_runs=1,
    tags=["sales", "project"],
    template_searchpath="/opt/airflow/dags/sql",
) as dag:

    synthetic_db_init = SQLExecuteQueryOperator(
        task_id="synthetic_db_init",
        sql="schema_init.sql",
        conn_id="postgres-dw",
        execution_timeout=timedelta(minutes=2),
    )

    customer_init = PythonOperator(
        task_id="customer_init",
        python_callable=_customer_init,
        execution_timeout=timedelta(minutes=4),
    )

    product_init = PythonOperator(
        task_id="product_init",
        python_callable=_product_init,
        execution_timeout=timedelta(minutes=4),
    )

    dim_date_init = PythonOperator(
        task_id="dim_date_init",
        python_callable=_dim_date_init,
        execution_timeout=timedelta(minutes=4),
    )

    transform_stg_dim_date = SQLExecuteQueryOperator(
        task_id="transform_stg_dim_date",
        sql="dim_date_init.sql",
        conn_id="postgres-dw",
        execution_timeout=timedelta(minutes=2),
    )

    synthetic_db_init >> [customer_init, product_init, dim_date_init]
    dim_date_init >> transform_stg_dim_date