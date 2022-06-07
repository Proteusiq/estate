from os import getenv
from minio import Minio

# from minio.error import S3Error

from dagster import AssetKey, IOManager, io_manager


S3_URI = getenv("DAGSTER_S3_URI")
ACCESS_KEY = getenv("DAGSTER_S3_ACCESS_KEY")
SECRET_KEY = getenv("DAGSTER_S3_SECRET_KEY")


def s3_client():
    # boto3.resource("s3", use_ssl=True).meta.client
    return Minio(S3_URI, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)


class S3IOManager(IOManager):
    def load_input(self, context):
        key = context.upstream_output.metadata["key"]
        bucket = context.resources.s3_bucket
        s3 = s3_client()

        if s3.bucket_exists(bucket):
            return s3.fget_object(bucket, key)
        return None

    def handle_output(self, context, obj):

        s3 = s3_client()
        key = context.metadata["key"]
        bucket = context.resource_config["bucket"]

        context.log.debug("about to save object")

        if not s3.bucket_exists(bucket):
            s3.make_bucket(bucket)

        s3.fput_object(bucket, key, "/data1")

    def get_output_asset_key(self, context):
        return AssetKey(["s3", context.resources.s3_bucket, context.metadata["key"]])


@io_manager(required_resource_keys={"s3_bucket"})
def fixed_s3_pickle_io_manager(_) -> S3IOManager:
    return S3IOManager()
