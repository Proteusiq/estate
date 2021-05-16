"""
Main example of using the design
    Note: Use for educational purposes only
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator

import pandas as pd

from webscrapers.estates import BoligaSold  # noqa
from webscrapers.estates import Estate  # noqa
from webscrapers.estates import Home  # noqa
from webscrapers.estates import Nybolig  # noqa
from customized.operators import ScrapEstateOperator, send_bolig  # noqa


args = {
    "owner": "Prayson",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup_by_default": False,
}


def home_scraper_section():
    """
    Create tasks in the outer section.
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

    home_ok = DummyOperator(task_id="home-ok")  # replace with with apicheck

    with TaskGroup("home_scraper") as home_scraper:
        _ = [
            ScrapEstateOperator(
                task_id=f"home-{i + 1}",
                url="https://home.dk/umbraco/backoffice/home-api/Search",
                api_name="home.dk",
                scraper_cls=Home,
                params=param,
            )
            for i, param in enumerate(params)
        ]

    home_ok >> home_scraper


# multipe pages per call
params = {
    "workers": 5,
    "start_page": 5,
    "end_page": 15,
    "pagesize": 15,
    "verbose": True,
}

with DAG(
    dag_id="example_from_home_estate_nybolig_boliga",
    description="Populate data from home.dk estate.dk and nybolig.dk",
    default_args=args,
    schedule_interval="@daily",
    start_date=datetime(2021, 5, 1),
    max_active_runs=4,
    tags=["estate_data"],
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("home", tooltip="Tasks for Home") as home:
        home_scraper_section()

    estate = ScrapEstateOperator(
        task_id="estate",
        url="https://www.estate.dk/Services/PropertySearch/Search",
        api_name="estate.dk",
        scraper_cls=Estate,
        params=params,
    )

    nybolig = ScrapEstateOperator(
        task_id="nybolig",
        url="https://www.nybolig.dk/Services/PropertySearch/Search",
        api_name="nybolig.dk",
        scraper_cls=Nybolig,
        params=params,
    )

    boliga_sold = ScrapEstateOperator(
        task_id="boliga_sold",
        url="https://api.boliga.dk/api/v2/sold/search/results",
        api_name="boliga.dk Sold",
        scraper_cls=BoligaSold,
        params=params,
    )

    end = DummyOperator(task_id="end")

    # task dependencies
    start >> home >> estate >> nybolig >> boliga_sold >> end
