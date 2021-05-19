from datetime import datetime
from typing import Optional
from airflow.decorators import dag, task
import pandas as pd
import httpx

from notification_senders.slack.sender import notify  # noqa
from helpers.dump_loader import dump_dataf  # noqa


default_args = {
    "owner": "Prayson",
    "catchup_by_default": False,
    "depends_on_past": False,
}


@dag(
    default_args=default_args,
    schedule_interval="3 * * * *",  # "*/30 * * * *"
    start_date=datetime(2021, 5, 1),
    catchup=False,
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
            res = dump_dataf(
                dataf=dataf, bucket_name=bucket_name, file_name=f"bolig_{postal}"
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

        _ = notify(
            task_id="price_notification",
            username="airflow_bot",
            status=True,  # success
            text=f"There are house to check: {get_result}",
            channel="#houseprices",
        )

    # my tasks
    response = get_houses(postal)
    notify_ = price_notification(response)  # noqa


find_price_dag = slack_price_notification(postal=2650)
