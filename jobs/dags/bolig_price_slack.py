from datetime import datetime, timedelta
from io import BytesIO
from os import environ
from typing import Optional
from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack import SlackAPIPostOperator  # noqa

from minio import Minio
import pandas as pd
import httpx

default_args = {
    "owner": "Prayson",
    "catchup_by_default": False,
}

minio_args = {
    "endpoint": environ.get("S3_ENDPOINT_URI", "minio:9000"),
    "access_key": environ.get("AWS_ACCESS_KEY_ID", "danpra"),
    "secret_key": environ.get("AWS_SECRET_ACCESS_KEY", "miniopwd"),
    "secure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    start_date=datetime.now() + timedelta(minutes=30),
    tags=["slack_me"],
)
def slack_price_notification(postal: Optional[int] = 2650, **kwargs):
    """Get Notified with fallen Prices

    Args:
        postal (Optional[int], optional): Danish Zip Code. Defaults to 2650.

    Raises:
        ConnectionError:  there is a failed connection
    """

    @task()
    def get_houses(postal: int, **kwargs) -> dict:
        """
        get villa houses in a postal code with fallen prices
        """
        defaults_params = {
            "pageSize": 200,
            "sort": "price-a",
            "propertyType": 1,  # 1 is villa
            "roomsMax": 4,
            "priceDevelopment": "down",
            "priceChangeDateMin": "2021-01-01 00:00:00",
            "includeds": 1,
            "zipCodes": postal,
        }

        defaults_params.update(kwargs)
        headers = {"user-agent": "Prayson W. Daniel: praysonpi@gmail.com"}
        URI = "https://api.boliga.dk/api/v2/search/results"

        r = httpx.get(URI, params=defaults_params, headers=headers)

        if r.status_code == 200:
            now = datetime.now()
            data = r.json()
        else:
            raise ConnectionError(
                f"failed to get data from {URI} with {defaults_params}!"
            )

        dataf = pd.DataFrame(data.get("results"))

        if not dataf.empty:
            bucket_name = "bolig-price"
            client = Minio(**minio_args)
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)

            dataf_json = dataf.to_json().encode("utf-8")

            res = client.put_object(
                bucket_name,
                f"bolig_{postal}_{now.strftime('%Y%m%d_%H%M%S')}.json",
                data=BytesIO(dataf_json),
                length=len(dataf_json),
                content_type="application/json",
            )

            return {
                "etag": res.etag,
                "bucket_name": res.bucket_name,
                "object_name": res.object_name,
            }
        else:

            return {
                "etag": None,
                "bucket_name": None,
                "object_name": None,
            }

    @task()
    def price_notification(get_result: dict) -> None:

        return SlackAPIPostOperator(
            task_id="price_notification",
            username="airflow_bot",
            token=environ.get("SLACK_TOKEN"),
            text=f"There are house to check: {get_result}",
            channel="#houseprices",
        ).execute()

    # my tasks
    response = get_houses(postal)
    notify = price_notification(response)  # noqa


find_price_dag = slack_price_notification(postal=2650)
