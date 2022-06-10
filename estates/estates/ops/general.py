from pandas import DataFrame
from dagster import op, AssetMaterialization, MetadataValue, Failure
from estates.bolig.io.sizeof import size_of_dataframe


@op(required_resource_keys={"warehouse"})
def store_dataframe(context, dataframe: DataFrame):
    """
    Load Home: Load dataframe to DataBase
    """

    context.log.info(f"Loading data {dataframe.shape} to Postgres ...")
    context.resources.warehouse.update_estate(dataframe)
    context.log.info("Loading data to completed")


@op
def prepare_dataframe(context, dataframe: DataFrame) -> DataFrame:

    """
    Prepare Home: Prepare data from real estate apis for upload to DataBase
    """

    if dataframe.empty:
        raise Failure(
            description="No dataframe to preprocess",
        )

    # postgres query roomSize will require "roomSize"
    dataframe.columns = dataframe.columns.str.lower()

    # columns with dict causes issues. stringfy thme
    columns = dataframe.select_dtypes("object").columns
    dataframe[columns] = dataframe[columns].astype(str)

    return dataframe


@op(required_resource_keys={"warehouse"})
def emit_dataframe_metadata(context, dataframe: DataFrame):

    """
    Emit Estate Data Metadata
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
