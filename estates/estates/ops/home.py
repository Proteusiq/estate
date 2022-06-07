from dagster import op, Field, Failure, Noneable
from pandas import DataFrame
from estates.bolig.core.scrappers import Home
from estates.bolig.scraper import ScrapEstate


@op(
    config_schema={
        "start_page": Field(int, is_required=False, default_value=1),
        "end_page": Field(Noneable(int), is_required=False, default_value=None),
        "pagesize": Field(int, is_required=False, default_value=15),
        "workers": Field(int, is_required=False, default_value=5),
        "verbose": Field(bool, is_required=False, default_value=True),
    }
)
def get_home(context) -> DataFrame:
    """
    Get home: Gather data from Home.dk
    """

    params = {
        "start_page": context.op_config.get("start_page"),
        "end_page": context.op_config.get("end_page"),
        "pagesize": context.op_config.get("pagesize"),
        "workers": context.op_config.get("workers"),
        "verbose": context.op_config.get("verbose"),
    }

    return ScrapEstate(
        url="https://home.dk/umbraco/backoffice/home-api/Search",
        api_name="home.dk",
        scraper_cls=Home,
        params=params,
    ).execute()


@op
def prepare_home(context, dataframe: DataFrame) -> DataFrame:

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
