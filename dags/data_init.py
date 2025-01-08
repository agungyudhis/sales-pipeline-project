from datetime import datetime, timedelta
from urllib.parse import quote_plus

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def customer_generator(n_samples):
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
                'id': f"ID-{str(cust_id).zfill(8)}",
                'name': name,
                'latitude': lat,
                'longitude': long,
                'place': place,
                'country': country_code
            }
        )
    return customer_list

def product_generator():
    import polars as pl
    
    product_name = {
        "Product A": 14.99, 
        "Product B": 19.99, 
        "Product C": 24.99,
        "Product D": 26.99,
    }
    sizes = {
        "S": -2.0, 
        "M": -1.0, 
        "L": 1.0, 
        "XL": 2.0
    }
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

def _customer_init():
    import json

    import polars as pl

    data = pl.DataFrame(customer_generator(50000))

    DB_CREDENTIALS = json.loads(Variable.get('db_credentials'))

    DB_HOST = DB_CREDENTIALS['host']
    DB_PORT = int(DB_CREDENTIALS['port'])
    DB_USER = DB_CREDENTIALS['user']
    DB_PASSWORD = DB_CREDENTIALS['password']

    db_uri = f'postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/warehouse'

    data.write_database('synthetic.customers', db_uri, if_table_exists='fail')

def _product_init():
    import json

    import polars as pl

    data = pl.DataFrame(product_generator())

    DB_CREDENTIALS = json.loads(Variable.get('db_credentials'))

    DB_HOST = DB_CREDENTIALS['host']
    DB_PORT = int(DB_CREDENTIALS['port'])
    DB_USER = DB_CREDENTIALS['user']
    DB_PASSWORD = DB_CREDENTIALS['password']

    db_uri = f'postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/warehouse'

    data.write_database('synthetic.products', db_uri, if_table_exists='fail')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    'data_init',
    default_args=default_args,
    description='Database and synthetic data initialization',
    schedule_interval='@once',
    start_date=datetime(2024, 12, 25),
    catchup=True,
    max_active_runs=1,
    tags=['sales', 'project'],
    template_searchpath="/opt/airflow/dags/sql",
) as dag:

    synthetic_db_init = SQLExecuteQueryOperator(
        task_id='synthetic_db_init',
        sql="schema_init.sql",
        conn_id='postgres-dw',
        execution_timeout=timedelta(minutes=2),
    )

    customer_init = PythonOperator(
        task_id='customer_init',
        python_callable=_customer_init,
        execution_timeout=timedelta(minutes=4),
    )

    product_init = PythonOperator(
        task_id='product_init',
        python_callable=_product_init,
        execution_timeout=timedelta(minutes=4),
    )

    synthetic_db_init >> [customer_init, product_init]