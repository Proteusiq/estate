from dagster import op, Field, Failure, Noneable
from pandas import DataFrame, concat
from estates.bolig.core.scrappers import Estate, Nybolig # noqa
from estates.bolig.scraper import ScrapEstate


@op(
    config_schema={
        "url": str,
        "start_page": Field(int, is_required=False, default_value=1),
        "end_page": Field(Noneable(int), is_required=False, default_value=None),
        "pagesize": Field(int, is_required=False, default_value=15),
        "workers": Field(int, is_required=False, default_value=5),
        "verbose": Field(bool, is_required=False, default_value=True),
    }
)
def get_service(context) -> DataFrame:
    """
    Get home: Gather data from Estate.dk
    """

    params = {
        "start_page": context.op_config.get("start_page"),
        "end_page": context.op_config.get("end_page"),
        "pagesize": context.op_config.get("pagesize"),
        "workers": context.op_config.get("workers"),
        "verbose": context.op_config.get("verbose"),
    }

    return ScrapEstate(
        url=context.op_config.get("url"),
        api_name="home.dk",
        scraper_cls=Estate,
        params=params,
    ).execute()


@op(
    config_schema={
        "ignore_index": Field(bool, is_required=False, default_value=True),
    }
)
def prepare_services(context, dataf_x: DataFrame, dataf_y: DataFrame) -> DataFrame:
    """
    Prepare Home: Prepare data from Home.dk for upload to DataBase
    """

    ignore_index = context.op_config.get("ignore_index")
    dataframe = concat((dataf_x, dataf_y), ignore_index=ignore_index)

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
