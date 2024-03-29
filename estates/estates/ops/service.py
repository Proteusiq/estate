from dagster import op, Field, Noneable
from pandas import DataFrame, concat
from estates.bolig.core.scrappers import Services
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
    Get estates: Gather data from [Nybolig|Estate].dk
    """

    params = {
        "start_page": context.op_config.get("start_page"),
        "end_page": context.op_config.get("end_page"),
        "pagesize": context.op_config.get("pagesize"),
        "workers": context.op_config.get("workers"),
        "verbose": context.op_config.get("verbose"),
    }

    url = context.op_config.get("url")

    return ScrapEstate(
        url=url,
        api_name=url[: url.find(".") + 2],
        scraper_cls=Services,
        params=params,
    ).execute()


@op(
    config_schema={
        "ignore_index": Field(bool, is_required=False, default_value=True),
    }
)
def prepare_service(context, dataf_x: DataFrame, dataf_y: DataFrame) -> DataFrame:
    """
    Prepare Home: Prepare data from Home.dk for upload to DataBase
    """

    ignore_index = context.op_config.get("ignore_index")
    dataframe = concat((dataf_x, dataf_y), ignore_index=ignore_index)

    return dataframe
