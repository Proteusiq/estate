from os import getenv
from pandas import DataFrame, read_sql
from sqlalchemy import create_engine
from dagster import resource, Field


POSTGRESS_URI = getenv("DAGSTER_PG_URI")


class SqlAlchemyPostgresWarehouse:
    def __init__(
        self,
        conn_str: str,
        table_name: str,
        if_exists: str,
    ):
        self._conn_str = conn_str
        self._engine = create_engine(self._conn_str)
        self._if_exists = if_exists
        self._table_name = table_name

    def update_estate(self, dataframe: DataFrame):
        dataframe.to_sql(
            self._table_name,
            con=self._engine,
            if_exists=self._if_exists,
            index=False,
        )

    def get_estates(self, sql_query: str) -> DataFrame:
        # TODO transform sql_query to models

        return read_sql(sql_query, con=self._engine)

    @property
    def table_name(self):
        return self._table_name


@resource(
    config_schema={
        "conn_str": Field(str, is_required=False, default_value=POSTGRESS_URI),
        "table_name": Field(str, is_required=False, default_value="home"),
        "if_exists": Field(str, is_required=False, default_value="replace"),
    }
)
def sqlalchemy_postgres_warehouse_resource(context):
    return SqlAlchemyPostgresWarehouse(**context.resource_config)
