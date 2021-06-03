from airflow.hooks.base_hook import BaseHook
import sqlalchemy
import pandas as pd
from helpers.loggers import logger  # noqa


CONNECTION_URI = BaseHook.get_connection("bolig_db").get_uri()


def send_bolig(bolig: pd.DataFrame, table: str, **kwargs) -> None:

    if not bolig.empty:
        return f"No DataFrame to send to {table}"

    # postgres query roomSize will require "roomSize"
    bolig.columns = bolig.columns.str.lower()

    # columns with dict causes issues. stringfy thme
    columns = bolig.select_dtypes("object").columns
    bolig[columns] = bolig[columns].astype(str)

    try:
        engine = sqlalchemy.create_engine(CONNECTION_URI)
        bolig.to_sql(table, engine, if_exists="append")
        logger.info(f"There were {len(bolig)} estates send to {table}")
    finally:
        engine.dispose()
