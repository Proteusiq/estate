from dagster import op, Field, Failure, Noneable
from pandas import DataFrame
from estates.bolig.core.scrappers import BoligaSold
from estates.bolig.scraper import ScrapEstate


@op(
    config_schema={
        "url": Field(str, is_required=False, default_value=None),
        "start_page": Field(int, is_required=False, default_value=1),
        "end_page": Field(Noneable(int), is_required=False, default_value=None),
        "pagesize": Field(int, is_required=False, default_value=15),
        "workers": Field(int, is_required=False, default_value=5),
        "verbose": Field(bool, is_required=False, default_value=True),
    }
)
def get_service(context) -> DataFrame:
    """
    Get estates: Gather data from [Nybolig|Estate].dk
    """

    params = {
        "start_page": context.op_config.get("start_page"),
        "end_page": context.op_config.get("end_page"),
        "pagesize": context.op_config.get("pagesize"),
        "workers": context.op_config.get("workers"),
        "verbose": context.op_config.get("verbose"),
    }

    url = context.op_config.get("url") or "https://api.boliga.dk/api/v2/sold/search/results"

    return ScrapEstate(
        url=url,
        api_name=url[: url.find(".") + 2],
        scraper_cls=BoligaSold,
        params=params,
    ).execute()


@op(
    config_schema={
        "ignore_index": Field(bool, is_required=False, default_value=True),
    }
)
def prepare_boliga(context, dataframe: DataFrame) -> DataFrame:
    """
    Prepare Home: Prepare data from Home.dk for upload to DataBase
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
