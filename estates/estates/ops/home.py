from dagster import op, Field, Noneable
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
