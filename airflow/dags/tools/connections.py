from airflow.hooks.base_hook import BaseHook
from tenacity import retry, wait_exponential


@retry(wait=wait_exponential(multiplier=1, min=5, max=60))
def fetch_connection_uri(uri_name):
    return BaseHook.get_connection(uri_name).get_uri()
