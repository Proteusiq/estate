from dagster import AssetGroup, asset
from pandas import DataFrame
from estates.warehouse.postgres import sqlalchemy_postgres_warehouse_resource


@asset(
    metadata={"owner": "prayson daniel", "domain": "estates", "priority": 3},
    required_resource_keys={"warehouse"},
)
def get_dataframe(context) -> DataFrame:

    sql_query = "SELECT * FROM services LIMIT 100"
    return context.resources.warehouse.get_estates(sql_query)


asset_group = AssetGroup(
    [
        get_dataframe,
    ],
    resource_defs={
        "warehouse": sqlalchemy_postgres_warehouse_resource,
    },
)

analysis_assets = asset_group.build_job(
    name="get_dataframe",
)
