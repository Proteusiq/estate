from pandas import DataFrame
from dagster import op, AssetMaterialization, MetadataValue
from estates.bolig.io.sizeof import size_of_dataframe


@op(required_resource_keys={"warehouse"})
def store_dataframe(context, dataframe: DataFrame):
    """
    Load Home: Load Home data to DataBase
    """

    context.log.info(f"Loading data {dataframe.shape} to Postgres ...")
    context.resources.warehouse.update_estate(dataframe)
    context.log.info("Loading data to completed")


@op(required_resource_keys={"warehouse"})
def emit_dataframe_metadata(context, dataframe: DataFrame):

    """
    Emit Home Data Size: Home metadata
    """
    context.log_event(
        AssetMaterialization(
            asset_key=context.resources.warehouse.table_name,
            metadata={
                "size (megabytes)": MetadataValue.text(size_of_dataframe(dataframe, unit="MB")),
                "number rows": MetadataValue.int(dataframe.shape[0]),
                "number columns": MetadataValue.int(dataframe.shape[1]),
                "# missing values": MetadataValue.int(int(dataframe.isna().sum().sum())),
                "# duplicate values": MetadataValue.int(int(dataframe.duplicated().sum())),
            },
        )
    )
