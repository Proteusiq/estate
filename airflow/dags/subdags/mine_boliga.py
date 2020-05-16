# Working progress

from collections import defaultdict
import os

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime

import sqlalchemy
import pandas as pd
import requests

from pipelines.boligax import BoligaRecent

CONNECTION_URI = BaseHook.get_connection('bolig_db').get_uri()
TABLE_NAME = 'recent_bolig'
POSTAL_TABLE = 'postal_codes'

# time https://airflow.apache.org/docs/1.10.3/_modules/airflow/utils/dates.html
args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}


def get_postal(**kwargs):
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    postal = pd.read_sql(f'SELECT "Postal" from {POSTAL_TABLE}', engine)
    return postal['Postal'].tolist()


def get_bolig(postal, **kwargs):

    engine = sqlalchemy.create_engine(CONNECTION_URI)
    bolig = BoligaRecent(url='https://api.boliga.dk/api/v2/search/results')

    bolig.get_pages(postal=postal, verbose=True)
    bolig.store.to_sql(TABLE_NAME, engine, if_exists='append')

    print(f'There were {len(bolig.store)} estates found in {postal}')
    engine.dispose()


def process_completed(**kwargs):
    print('Mining Completed')
    return f'Completed'


with DAG(
    dag_id='coming_soon_populate_estates',
    description=f'Populate estates to {TABLE_NAME} working on flow design',
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval='30 23 * * 1-5',
) as dag:

    push_bolig_data = [PythonOperator(
        task_id=f'load_{postal}_bolig_data',
        python_callable=get_bolig,
        op_args=postal,
        dag=dag,
        provide_context=True

    ) for postal in get_postal()]

    get_postal = PythonOperator(
        task_id='get_postal',
        dag=dag,
        python_callable=get_postal,
        provide_context=True
    )

    process_completed = PythonOperator(
        task_id='mining_completed',
        dag=dag,
        python_callable=process_completed,
        provide_context=True
    )


# get_postal >> push_bolig_data >> process_completed
