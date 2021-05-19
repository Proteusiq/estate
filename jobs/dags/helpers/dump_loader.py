from io import BytesIO
from os import environ
from datetime import datetime
from minio import Minio
import pandas as pd

# TODO
# Add function docstring

MINIO_ARGS = {
    "endpoint": environ.get("S3_ENDPOINT_URI", "minio:9000"),
    "access_key": environ.get("AWS_ACCESS_KEY_ID", "danpra"),
    "secret_key": environ.get("AWS_SECRET_ACCESS_KEY", "miniopwd"),
    "secure": False,
}


def dump_dataf(dataf: pd.DataFrame, bucket_name: str, file_name: str):

    if dataf.empty:
        raise ValueError("DataFrame cannot be empty!")
    client = Minio(**MINIO_ARGS)
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    dataf_json = dataf.to_json().encode("utf-8")
    now = datetime.now()

    res = client.put_object(
        bucket_name,
        f"{file_name}_{now.strftime('%Y%m%d_%H%M%S')}.json",
        data=BytesIO(dataf_json),
        length=len(dataf_json),
        content_type="application/json",
    )

    return res


def load_dataf(bucket_name: str, file_name: str):

    client = Minio(**MINIO_ARGS)
    res = client.get_object(bucket_name, file_name)

    return pd.read_json(BytesIO(res.data))