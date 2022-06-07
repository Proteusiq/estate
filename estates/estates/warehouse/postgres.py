from sqlalchemy import engine as Engine
from pandas import DataFrame
from dagster import resource, Field


class SqlAlchemyPostgresWarehouse:
    def __init__(
        self,
        table_name: str,
        if_exists: str,
    ):

        self._if_exists = if_exists
        self._table_name = table_name

    def update_estate(self, dataframe: DataFrame, engine: Engine):
        dataframe.to_sql(
            self._table_name,
            con=engine,
            if_exists=self._if_exists,
            index=False,
        )

    @property
    def table_name(self):
        return self._table_name


@resource(
    config_schema={
        "table_name": Field(str, is_required=False, default_value="home"),
        "if_exists": Field(str, is_required=False, default_value="replace"),
    }
)
def sqlalchemy_postgres_warehouse_resource(context):
    return SqlAlchemyPostgresWarehouse(**context.resource_config)
