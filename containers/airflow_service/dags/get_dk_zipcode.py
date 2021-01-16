"""
This script contains an example of populating Danish zipcodes from dawa api
"""


from collections import defaultdict
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime
import pandas as pd
import requests
import sqlalchemy
from tools.connections import fetch_connection_uri


CONNECTION_URI = next(fetch_connection_uri("bolig_db"))
TABLE_NAME = "postal_codes"


args = {
    "owner": "Prayson",
    "catchup_by_default": False,
}


def get_postal(only_postal: bool = True, **kwargs) -> pd.DataFrame:
    """Populata Danish zipcodes
    Simple function that returns DK postal codes for DAWA API.

    Keyword Arguments:
        only_postal {bool} -- return only zipcode without metadata (default: {True})

    Returns:
        pd.DataFrame -- DataFrame with all danish zipcodes

    Usage:

        >>> zipcodes = get_postal(only_postal=False)
    """

    post_data = defaultdict(list)
    URI = "http://dawa.aws.dk/postnumre"

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

    return f"postal data pushed to {TABLE_NAME}"


def check_postal(**kwargs):
    """check if the postal code database exists and populates"""

    engine = sqlalchemy.create_engine(CONNECTION_URI)

    print(f"checking if postal data is in {TABLE_NAME}")

    try:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)

        print(f"postal data with {df.shape} exits")
        engine.dispose()

    except sqlalchemy.exc.ProgrammingError as e:
        print(e.__dict__["statement"])
        return False

    return True


with DAG(
    dag_id="populate_postal_codes",
    description=f"Populate postal code to {TABLE_NAME}",
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval="@once",
) as dag:

    push_postal_data = PythonOperator(
        task_id="load_postal_data",
        python_callable=get_postal,
        op_args=[
            False,
        ],
        dag=dag,
        provide_context=True,
    )

    check_postal_data = PythonOperator(
        task_id="check_postal_data",
        dag=dag,
        python_callable=check_postal,
        provide_context=True,
    )


push_postal_data >> check_postal_data
