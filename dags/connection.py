from urllib.parse import quote_plus

import sqlalchemy
from airflow.models import Variable


def create_engine(
    cred_variable: str = "db_credentials", database: str = "warehouse"
) -> sqlalchemy.engine.Engine:
    import json

    # Load postgres db credentials from airflow variables
    DB_CREDENTIALS = json.loads(Variable.get(cred_variable))

    DB_HOST = DB_CREDENTIALS["host"]
    DB_PORT = int(DB_CREDENTIALS["port"])
    DB_USER = DB_CREDENTIALS["user"]
    DB_PASSWORD = DB_CREDENTIALS["password"]

    # Create sqlsalchemy engine to connect the postgres db
    return sqlalchemy.create_engine(
        f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{database}"
    )
