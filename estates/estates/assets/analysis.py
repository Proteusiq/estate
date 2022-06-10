from dagster import AssetGroup, asset, AssetMaterialization, MetadataValue
from pandas import DataFrame
from estates.warehouse.postgres import sqlalchemy_postgres_warehouse_resource


@asset(
    metadata={"owner": "prayson daniel", "domain": "estates", "priority": 3},
    required_resource_keys={"warehouse"},
    namespace=["datasets"],
    compute_kind="python",
)
def get_dataframe(context) -> DataFrame:
    """Estates Data

    Get estate data from multiple sources for price prediction model
    """

    if context.resources.warehouse.sql_query is None:
        SQL_QUERY = """\
        SELECT * FROM services;
        """
        context.resources.warehouse.sql_query = SQL_QUERY

    dataframe = context.resources.warehouse.get_estates()

    context.log_event(
        AssetMaterialization(
            asset_key=context.resources.warehouse.table_name,
            metadata={
                "# of rows": MetadataValue.int(dataframe.shape[0]),
                "# of columns": MetadataValue.int(dataframe.shape[1]),
                "# missing values": MetadataValue.int(int(dataframe.isna().sum().sum())),
                "# duplicate values": MetadataValue.int(int(dataframe.duplicated().sum())),
            },
        )
    )
    return dataframe


asset_group = AssetGroup(
    [
        get_dataframe,
    ],
    resource_defs={
        "warehouse": sqlalchemy_postgres_warehouse_resource,
    },
)

analysis_assets = asset_group.build_job(
    name="get_estate_dataframe",
)
