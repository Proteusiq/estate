from functools import wraps, partial
from typing import Callable
from pandas import read_sql, DataFrame
from sqlalchemy import create_engine, engine as Engine
from dagster import IOManager, io_manager, Field


def connection(conn_str: str) -> Callable:
    def outer(function: Callable):
        @wraps(function)
        def inner(*args, **kwargs):

            engine = create_engine(conn_str)
            partial_function = partial(function, conn=engine)

            return partial_function

        return inner

    return outer


@connection(conn_str="sqlite://")  # in ":memory:"
def write_dataframe_to_sqlite(name: str, dataframe: DataFrame, conn: Engine) -> None:
    dataframe.to_sql(name, conn, if_exists="replace", index=False)


@connection(conn_str="sqlite://")
def read_dataframe_from_sqlite(name: str, conn: Engine) -> DataFrame:
    return read_sql(name, conn)


class DataframeTableIOManager(IOManager):
    def handle_output(self, context, obj):

        self._table_name = context.config["table_name"]
        write_dataframe_to_sqlite(name=self._table_name, dataframe=obj)

    def load_input(self, context):
        self._table_name = context.upstream_output.config["table_name"]
        return read_dataframe_from_sqlite(name=self._table_name)

    @property
    def table_name(self) -> str:
        return self._table_name


@io_manager(
    output_config_schema={"table_name": Field(str, is_required=False, default_value="home")},
)
def dataframe_sqlite_io_manager(_):
    return DataframeTableIOManager()
