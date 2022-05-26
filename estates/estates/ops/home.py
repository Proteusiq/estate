from dagster import op
from estates.bolig.core.scrappers import Home  # noqa
from estates.bolig.scraper import ScrapEstate



@op
def get_home() -> list[dict]:
    """
    An op definition. This example op outputs a single string.

    For more hints about writing Dagster ops, see our documentation overview on Ops:
    https://docs.dagster.io/concepts/ops-jobs-graphs/ops
    """

    params = (
        {
            "workers": 5,
            "start_page": start_page,
            "end_page": end_page,
            "pagesize": 15,
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
