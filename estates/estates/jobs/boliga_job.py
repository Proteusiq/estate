from dagster import job
from estates.ops.boliga import get_boliga
from estates.ops.general import store_dataframe, emit_dataframe_metadata, prepare_dataframe
from estates.warehouse.postgres import sqlalchemy_postgres_warehouse_resource


@job(
    resource_defs={
        "warehouse": sqlalchemy_postgres_warehouse_resource,
    }
)
def make_boliga_job():
    """
    Boliga Job

    Get data from boliga api https://api.boliga.dk/api/v2/

    """
    boliga_dataframe = prepare_dataframe(get_boliga())
    store_dataframe(boliga_dataframe)
    emit_dataframe_metadata(boliga_dataframe)
