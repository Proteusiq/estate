from dagster import job
from estates.ops.service import get_service, prepare_service
from estates.ops.general import store_dataframe, emit_dataframe_metadata, prepare_dataframe
from estates.warehouse.postgres import sqlalchemy_postgres_warehouse_resource


@job(
    resource_defs={
        "warehouse": sqlalchemy_postgres_warehouse_resource,
    }
)
def make_service_job():
    """
    Get Data Frome Estates|Nybolig.dk api
    
    """
    service_dataframe = prepare_service(get_service.alias("estate_service")(), get_service.alias("nybolig_service")())
    dataframe = prepare_dataframe(service_dataframe)
    store_dataframe(dataframe)
    emit_dataframe_metadata(dataframe)
