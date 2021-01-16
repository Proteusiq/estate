from airflow.hooks.base_hook import BaseHook
from tenacity import retry, wait_exponential


@retry(wait=wait_exponential(multiplier=1, min=5, max=60))
def fetch_connection_uri(uri_name):
    # never use return. we want other processes to continue as we wait
    yield BaseHook.get_connection(uri_name).get_uri()
