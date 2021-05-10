"""
This script contains an example of populating Danish zipcodes from dawa api
"""


from collections import defaultdict

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

import pandas as pd
import requests
import sqlalchemy


CONNECTION_URI = BaseHook.get_connection("bolig_db").get_uri()
TABLE_NAME = "postal_codes"


default_args = {
    "owner": "Prayson",
    "catchup_by_default": False,
}


@dag(
    default_args=default_args,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=["postal_data"],
)
def get_postal(only_postal: bool = True, **kwargs) -> bool:
    """Populata Danish zipcodes
    Simple function that returns DK postal codes for DAWA API.

    Keyword Arguments:
        only_postal {bool} -- return only zipcode without metadata (default: {True})

    Returns:
        pd.DataFrame -- DataFrame with all danish zipcodes

    Usage:

        >>> zipcodes = get_postal(only_postal=False)
    """

    @task(multiple_outputs=True)
    def get_data(only_postal: bool = only_postal) -> str:
        post_data = defaultdict(list)
        URI = "https://api.dataforsyningen.dk/postnumre"

        with requests.Session() as httpx:
            r = httpx.get(URI)

        assert r.ok, "Connection Error"

        posts = r.json()

        for _, post in enumerate(posts):
            post_data["Postal"].append(post["nr"])
            post_data["Name"].append(post["navn"])
            post_data["Kommuner"].append(
                post["kommuner"][0]["navn"] if post["kommuner"] else None
            )
            post_data["Longitude"].append(post["visueltcenter"][0])
            post_data["Latitude"].append(post["visueltcenter"][1])
            # post_data['bbox'].append(post['bbox'])

        if only_postal:
            df = pd.DataFrame(post_data)[["Postal"]]
        else:
            df = pd.DataFrame(post_data)

        df.columns = df.columns.str.lower()
        df["postal"] = df["postal"].astype(int)

        engine = sqlalchemy.create_engine(CONNECTION_URI)
        df.to_sql(TABLE_NAME, engine, if_exists="replace")
        engine.dispose()

        return {
            "ncolums": df.shape[1],
            "nrows": df.shape[0],
            "db": CONNECTION_URI.split("/")[-1],
            "table_name": TABLE_NAME,
        }

    @task
    def check_data(get_result: dict) -> bool:
        """check if the postal code database exists and populates"""

        engine = sqlalchemy.create_engine(CONNECTION_URI)
        table_name = get_result.get("table_name")
        print(f"checking if postal data is in {table_name}")

        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

            print(f"{table_name} data with {df.shape} exits")
            engine.dispose()

        except sqlalchemy.exc.ProgrammingError as e:
            print(e.__dict__["statement"])
            return False

        return True

    # pipeline
    postal_data = get_data(only_postal)
    check_data(postal_data)


load_postal_dag = get_postal(
    only_postal=True,
)
