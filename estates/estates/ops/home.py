from dagster import op
from estates.bolig.core.scrappers import Home  # noqa
from estates.bolig.scraper import ScrapEstate



@op(config_schema={"workers": int, "pagesize": int})
def get_home(context) -> list[dict]:
    """
    An op definition. This example op outputs a single string.

    For more hints about writing Dagster ops, see our documentation overview on Ops:
    https://docs.dagster.io/concepts/ops-jobs-graphs/ops
    """

    worker = context.op_config.get("workers", 5)
    pagesize = context.op_config.get("pagesize", 15)

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
