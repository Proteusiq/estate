from os import getenv
from typing import Optional
from pandas import DataFrame, read_sql
from sqlalchemy import create_engine
from dagster import resource, Field, Noneable


POSTGRESS_URI = getenv("DAGSTER_PG_URI")


class SqlAlchemyPostgresWarehouse:
    def __init__(
        self,
        conn_str: str,
        table_name: str,
        if_exists: str,
        sql_query: Optional[str] = None,
    ):
        self._conn_str = conn_str
        self._engine = create_engine(self._conn_str)
        self._if_exists = if_exists
        self._table_name = table_name
        self._sql_query = sql_query

    def update_estate(self, dataframe: DataFrame):
        dataframe.to_sql(
            self._table_name,
            con=self._engine,
            if_exists=self._if_exists,
            index=False,
        )

    def get_estates(self) -> DataFrame:
        # TODO transform sql_query to models

        dataframe = read_sql(self._sql_query, con=self._engine)
        return dataframe

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def sql_query(self) -> str:
        # TODO transform sql_query to models
        return self._sql_query

    @sql_query.setter
    def sql_query(self, sql_query) -> None:
        self._sql_query = sql_query


@resource(
    config_schema={
        "conn_str": Field(str, is_required=False, default_value=POSTGRESS_URI),
        "table_name": Field(str, is_required=False, default_value="home"),
        "if_exists": Field(str, is_required=False, default_value="replace"),
        "sql_query": Field(Noneable(str), is_required=False, default_value=None),
    }
)
def sqlalchemy_postgres_warehouse_resource(context):
    return SqlAlchemyPostgresWarehouse(**context.resource_config)
