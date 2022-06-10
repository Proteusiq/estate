from dagster import op, Field, Noneable
from pandas import DataFrame
from estates.bolig.core.scrappers import Boliga
from estates.bolig.scraper import ScrapEstate


@op(
    config_schema={
        "url": Field(Noneable(str), is_required=False, default_value=None),
        "start_page": Field(int, is_required=False, default_value=1),
        "end_page": Field(Noneable(int), is_required=False, default_value=None),
        "pagesize": Field(int, is_required=False, default_value=15),
        "workers": Field(int, is_required=False, default_value=5),
        "verbose": Field(bool, is_required=False, default_value=True),
    }
)
def get_boliga(context) -> DataFrame:
    """
    Get estates: Gather data from boliga.dk
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
        api_name="api.boliga.dk",
        scraper_cls=Boliga,
        params=params,
    ).execute()
