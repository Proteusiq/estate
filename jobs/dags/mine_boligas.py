"""
[Deprecated]: `pipelines.boligax` is deprecated. Use `pipelines.boliger.boliga`
This script contains an example of self generating tasks from UI Variable
input of multiple zipcodes

Contains two major codes: thread and unnessary data copy: bug > pd.DataFrame is not thread safe, flow: pd.append used in loop 
"""

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import datetime
import sqlalchemy
import pandas as pd
from pipelines.boligax import BoligaRecent


CONNECTION_URI = BaseHook.get_connection("bolig_db").get_uri()
TABLE_NAME = "recent_boligs"


# default args
args = {
    "owner": "Prayson",
    "catchup_by_default": False,
}


def get_bolig(postal, **kwargs):
    """get bolig[estate]

    Arguments:
        postal {[type]} -- [description]
    """

    engine = sqlalchemy.create_engine(CONNECTION_URI)
    bolig = BoligaRecent(url="https://api.boliga.dk/api/v2/search/results")

    bolig.get_pages(postal=postal, verbose=True)
    if not bolig.store.empty:
        # columns with dict causes issues. stringfy thme
        columns = bolig.store.select_dtypes("object").columns
        bolig.store[columns] = bolig.store[columns].astype(str)
        bolig.store.columns = (
            bolig.store.columns.str.lower()
        )  # postgres query roomSize will require "roomSize"
        bolig.store.to_sql(TABLE_NAME, engine, if_exists="append")

        print(f"There were {len(bolig.store)} estates found in {postal}")
        engine.dispose()
    else:
        print(f"There were no estates found in {postal}")


def process_notify(engine=None, **kwargs):
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
    engine.dispose()
    print(f"Mining {TABLE_NAME} has {len(df)} rows at {datetime.now()}")
    return f"Data sending completed"


# if the Variable are not entered in UI Variable key:postals value: {"whatever": 2650,"bla":2400}
DEFAULT_POSTAL = {"1": 2200, "2": 2450}

postals = Variable.get("postals", DEFAULT_POSTAL, deserialize_json=True)
with DAG(
    dag_id="multiple_postals_estates",
    description=f"Populate estates to {TABLE_NAME} working on flow design",
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval="30 23 * * 1-5",
) as dag:

    push_bolig_data = [
        PythonOperator(
            task_id=f"load_{postal}_bolig_data",
            python_callable=get_bolig,
            op_args=[
                postal,
            ],
            dag=dag,
            provide_context=True,
        )
        for postal in [post for _, post in postals.items()]
    ]

    process_completed = PythonOperator(
        task_id="mining_completed",
        dag=dag,
        python_callable=process_notify,
        provide_context=True,
    )


push_bolig_data >> process_completed
