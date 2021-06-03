from datetime import datetime, timedelta
from typing import Optional, Any
import httpx
import pandas as pd
from airflow.decorators import dag, task

from notification_senders.slack.sender import notify  # noqa
from helpers.dump_loader import dump_dataf  # noqa

# fartkontrol service


default_args = {
    "owner": "Prayson",
    "catchup_by_default": False,
}


@dag(
    default_args=default_args,
    schedule_interval="1 * * * *",
    start_date=datetime(2021, 5, 1),
    catchup=False,
    tags=["speed_traps"],
)
def get_fartkontrol_station(
    # latitude: Optional[Any] = 0.0, longitude: Optional[Any] = 0.0, **kwargs
):
    @task()
    def get_camera_traps(latitude=0, longitude=0, **kwargs) -> dict:
        URI = "https://www.fartkontrol.nu/ajax.php"
        PARAMS = {
            "json": f"""{{"action":"controls_fetch","uid":null,"app":0,"my_coords":{{"Ya":{latitude},"Za":{longitude},"ac":0}},
        "types":["1","-1","-1",null,"1"]}}"""
        }

        r = httpx.get(
            URI,
            params=PARAMS,
        )

        if r.status_code == 200:
            data = r.json()
        else:
            raise ConnectionError(f"failed to get data from {URI} with {PARAMS}!")

        speed_controls_df = pd.DataFrame(data["controls"].get("coordinates")).T
        speed_controls_df["requests_time"] = data["controls"].get("time")
        speed_controls_df = speed_controls_df.reset_index(drop=True)

        # dump data to Minio|S3
        res = dump_dataf(
            dataf=speed_controls_df,
            bucket_name="speed-control",
            file_name="speed_traps",
        )

        return {
            "etag": res.etag,
            "bucket_name": res.bucket_name,
            "object_name": res.object_name,
        }

    @task()
    def traps_notification(get_result: dict) -> None:
        _ = notify(
            task_id="trap_notification",
            username="airflow_bot",
            status=True,  # success
            text=f"Check traps: {get_result}",
            channel="#speedtraps",
        )

    get_traps = get_camera_traps(latitude=0, longitude=0)
    _ = traps_notification(get_traps)


speed_notification = get_fartkontrol_station()
