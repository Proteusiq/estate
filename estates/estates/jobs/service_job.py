from dagster import job
from estates.ops.service import get_service, prepare_service
from estates.ops.general import store_dataframe, emit_dataframe_metadata
from estates.warehouse.postgres import sqlalchemy_postgres_warehouse_resource


@job(
    resource_defs={
        "warehouse": sqlalchemy_postgres_warehouse_resource,
    }
)
def make_service_job():
    """
    A job definition. This example job has a single op.

    For more hints on writing Dagster jobs, see our documentation overview on Jobs:
    https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs
    """
    service_dataframe = prepare_service(get_service.alias("estate_service")(), get_service.alias("nybolig_service")())
    store_dataframe(service_dataframe)
    emit_dataframe_metadata(service_dataframe)
