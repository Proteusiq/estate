from dagster import op, Field
from estates.bolig.core.scrappers import Home
from estates.bolig.scraper import ScrapEstate


@op(
    config_schema={
        "workers": Field(int, is_required=False, default_value=5),
        "pagesize": Field(int, is_required=False, default_value=15),
    }
)
def get_home(context) -> list[dict]:
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
