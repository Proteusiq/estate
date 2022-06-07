from os import getenv
from contextlib import contextmanager
from sqlalchemy import create_engine, engine as Engine
from dagster import resource, Field


POSTGRESS_URI = getenv("DAGSTER_PG_URI")


@resource(
    config_schema={
        "conn_str": Field(str, is_required=False, default_value=POSTGRESS_URI),
    }
)
@contextmanager
def database_connection(conn_str: str) -> Engine:
    try:
        connection = create_engine(conn_str)

        yield connection
    finally:
        connection.dispose()
