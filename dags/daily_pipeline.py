import sys

sys.path.insert(0, "/opt/airflow/dags")

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from connection import create_engine
from data_generator import generate_orders, generate_traffic_data


def upload_to_minio(bucket, folder, data, filename):
    import io
    import json

    import boto3

    minio_api_key = json.loads(Variable.get("minio_api_key"))

    endpoint_url = minio_api_key["endpoint_url"]
    access_key = minio_api_key["access_key"]
    secret_key = minio_api_key["secret_key"]

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    with io.StringIO() as file_buffer:
        json.dump(data, file_buffer)

        # Upload a file to the bucket
        response_upload = s3.put_object(
            Bucket=bucket, Key=f"{folder}/{filename}", Body=file_buffer.getvalue()
        )

        status = response_upload.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Failed to upload file to MinIO. Status - {status}")
            response_upload.raise_for_status()


def _extract_orders(task_instance: TaskInstance | None = None) -> str:

    START_DATE = task_instance.execution_date - timedelta(hours=4)
    END_DATE = task_instance.execution_date

    # Simulating an API response
    data = generate_orders(START_DATE, END_DATE)

    if data["order_count"]:
        upload_to_minio(
            "raw", "orders", data, START_DATE.strftime(r"%Y%m%d%H%M") + ".json"
        )
        return "load_orders"
    else:
        return "skip_empty"


def _extract_web_traffic(task_instance: TaskInstance | None = None) -> str:

    START_DATE = task_instance.execution_date - timedelta(hours=4)
    END_DATE = task_instance.execution_date

    # Simulating an API response
    data = generate_traffic_data(START_DATE, END_DATE)

    if data["visitor_count"]:
        upload_to_minio(
            "raw", "traffic", data, START_DATE.strftime(r"%Y%m%d%H%M") + ".json"
        )
        return "load_web_traffic"
    else:
        return "skip_empty"


def s3_decorator(
    bucket, folder, minio_credentials_variable, pg_database, pg_schema, pg_table
):
    def inner(func):
        def wrapper(*args, **kwargs):
            import sys

            sys.path.insert(0, "/opt/airflow/dags")
            import io
            import json

            import boto3
            import polars as pl

            # from schema import Orders

            minio_api_key = json.loads(Variable.get(minio_credentials_variable))
            endpoint_url = minio_api_key["endpoint_url"]
            access_key = minio_api_key["access_key"]
            secret_key = minio_api_key["secret_key"]

            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

            response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{folder}/")

            if len(response["Contents"]):
                all_data = pl.DataFrame()
                for file in sorted(response["Contents"], key=lambda d: d["Key"]):
                    if file["Key"][-5:] == ".json":
                        response_object = s3.get_object(Bucket="raw", Key=file["Key"])
                        data_list = json.load(
                            io.BytesIO(response_object["Body"].read())
                        )

                        data = func(data_list)

                        all_data = pl.concat([all_data, data], how="diagonal")

                        s3.put_object(
                            Bucket="archive",
                            Key=file["Key"],
                            Body=json.dumps(data_list),
                        )

                # Create sqlsalchemy engine to connect the postgres db
                engine = create_engine(
                    cred_variable="db_credentials", database=pg_database
                )

                with engine.connect() as connection:
                    all_data.write_database(
                        f"{pg_schema}.{pg_table}", connection, if_table_exists="replace"
                    )

                delete_response = s3.delete_objects(
                    Bucket=bucket,
                    Delete={
                        "Objects": [
                            {"Key": item["Key"]}
                            for item in response["Contents"]
                            if item["Key"][-5:] == ".json"
                        ]
                    },
                )
                delete_status = delete_response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode"
                )

                if delete_status == 200:
                    print(
                        f"Successful S3 delete_objects response. Status - {delete_status}"
                    )
                else:
                    print(
                        f"Unsuccessful S3 delete_objects response. Status - {delete_status}"
                    )
                    delete_response.raise_for_status()

        return wrapper

    return inner


@s3_decorator(
    bucket="raw",
    folder="orders",
    minio_credentials_variable="minio_api_key",
    pg_database="warehouse",
    pg_schema="staging",
    pg_table="stg_orders",
)
def _load_orders(data_list):
    import polars as pl
    from schema import Orders

    data = (
        pl.DataFrame([Orders(**item).model_dump() for item in data_list["orders"]])
        .explode("item_list")
        .unnest("item_list")
    )

    return data


@s3_decorator(
    bucket="raw",
    folder="traffic",
    minio_credentials_variable="minio_api_key",
    pg_database="warehouse",
    pg_schema="staging",
    pg_table="stg_traffic",
)
def _load_web_traffic(data_list):
    import polars as pl
    from schema import Visitors

    data = pl.DataFrame(
        [Visitors(**item).model_dump() for item in data_list["visitors"]]
    )

    return data


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
    "daily_pipeline",
    default_args=default_args,
    description="Daily batch pipeline",
    schedule_interval="0 0 * * *",
    start_date=datetime(2022, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sales", "project", "pipeline"],
    template_searchpath="/opt/airflow/dags/sql",
) as dag:

    extract_orders = BranchPythonOperator(
        task_id="extract_orders",
        python_callable=_extract_orders,
        execution_timeout=timedelta(minutes=15),
    )

    extract_web_traffic = BranchPythonOperator(
        task_id="extract_web_traffic",
        python_callable=_extract_web_traffic,
        execution_timeout=timedelta(minutes=15),
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=_load_orders,
        execution_timeout=timedelta(minutes=15),
    )

    load_web_traffic = PythonOperator(
        task_id="load_web_traffic",
        python_callable=_load_web_traffic,
        execution_timeout=timedelta(minutes=15),
    )

    skip_empty = EmptyOperator(task_id="skip_empty")

    with TaskGroup(group_id="sales_dim_update") as sales_dim_update:
        dim_location_update = SQLExecuteQueryOperator(
            task_id="dim_location_update",
            sql="dim_location_update.sql",
            conn_id="postgres-dw",
            execution_timeout=timedelta(minutes=15),
        )

        dim_customer_update = SQLExecuteQueryOperator(
            task_id="dim_customer_update",
            sql="dim_customer_update.sql",
            conn_id="postgres-dw",
            execution_timeout=timedelta(minutes=15),
        )

        dim_product_update = SQLExecuteQueryOperator(
            task_id="dim_product_update",
            sql="dim_product_update.sql",
            conn_id="postgres-dw",
            execution_timeout=timedelta(minutes=15),
        )

        dim_location_update >> dim_customer_update
        dim_product_update

    with TaskGroup(group_id="web_dim_update") as web_dim_update:
        dim_device_update = SQLExecuteQueryOperator(
            task_id="dim_device_update",
            sql="dim_device_update.sql",
            conn_id="postgres-dw",
            execution_timeout=timedelta(minutes=15),
        )

        dim_channel_update = SQLExecuteQueryOperator(
            task_id="dim_channel_update",
            sql="dim_channel_update.sql",
            conn_id="postgres-dw",
            execution_timeout=timedelta(minutes=15),
        )

    fct_sales_update = SQLExecuteQueryOperator(
        task_id="fct_sales_update",
        sql="fct_sales_update.sql",
        conn_id="postgres-dw",
        execution_timeout=timedelta(minutes=15),
    )

    fct_traffic_update = SQLExecuteQueryOperator(
        task_id="fct_traffic_update",
        sql="fct_traffic_update.sql",
        conn_id="postgres-dw",
        execution_timeout=timedelta(minutes=15),
    )

    fct_periodic_summary_update = SQLExecuteQueryOperator(
        task_id="fct_periodic_summary_update",
        sql="fct_periodic_summary_update.sql",
        conn_id="postgres-dw",
        execution_timeout=timedelta(minutes=15),
        trigger_rule='none_failed'
    )

    extract_orders >> [load_orders, skip_empty]
    extract_web_traffic >> [load_web_traffic, skip_empty]

    load_orders >> sales_dim_update >> fct_sales_update
    load_web_traffic >> web_dim_update >> fct_traffic_update
    
    [fct_sales_update, fct_traffic_update] >> fct_periodic_summary_update
