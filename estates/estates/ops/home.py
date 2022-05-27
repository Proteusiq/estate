from dagster import op, Field
from pandas import DataFrame, concat
from estates.bolig.core.scrappers import Home
from estates.bolig.scraper import ScrapEstate


@op(
    config_schema={
        "workers": Field(int, is_required=False, default_value=5),
        "pagesize": Field(int, is_required=False, default_value=15),
    }
)
def get_home(context) -> list[DataFrame]:
    """
    Get home
    """

    worker = context.op_config.get("workers")
    pagesize = context.op_config.get("pagesize")

    params = (
        {
            "workers": worker,
            "start_page": start_page,
            "end_page": end_page,
            "pagesize": pagesize,
            "verbose": True,
        }
        for start_page, end_page in {(1, 5), (5, 15), (15, 20)}
    )

    data = [
        ScrapEstate(
            task_id=f"home-{i + 1}",
            url="https://home.dk/umbraco/backoffice/home-api/Search",
            api_name="home.dk",
            scraper_cls=Home,
            params=param,
        ).execute()
        for i, param in enumerate(params)
    ]
    return data


@op(
    config_schema={
        "ignore_index": Field(bool, is_required=False, default_value=True),
    }
)
def prepare_home(context, dataframes: list[DataFrame]) -> DataFrame:

    ignore_index = context.op_config.get("ignore_index")
    dataframe = concat(dataframes, ignore_index=ignore_index)

    if dataframe.empty:
        raise ValueError("No DataFrame to send process")

    # postgres query roomSize will require "roomSize"
    dataframe.columns = dataframe.columns.str.lower()

    # columns with dict causes issues. stringfy thme
    columns = dataframe.select_dtypes("object").columns
    dataframe[columns] = dataframe[columns].astype(str)

    return dataframe


@op(required_resource_keys={"warehouse"})
def store_home(context, dataframe: DataFrame):

    context.log.info(f"Loading data {dataframe.shape} to Postgres ...")
    context.resources.warehouse.update_estate(dataframe)
    context.log.info("Loading data to completed")
