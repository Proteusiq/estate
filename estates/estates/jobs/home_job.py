from dagster import job
from estates.ops.home import get_home
from estates.ops.general import store_dataframe, emit_dataframe_metadata, prepare_dataframe
from estates.warehouse.postgres import sqlalchemy_postgres_warehouse_resource


@job(
    resource_defs={
        "warehouse": sqlalchemy_postgres_warehouse_resource,
    }
)
def make_home_job():
    """
    A job definition. This example job has a single op.

    For more hints on writing Dagster jobs, see our documentation overview on Jobs:
    https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs
    """
    home_dataframe = prepare_dataframe(get_home())
    store_dataframe(home_dataframe)
    emit_dataframe_metadata(home_dataframe)
