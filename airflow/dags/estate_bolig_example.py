'''
Main example of using the design
    Note: Use for educational purposes only
'''

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import datetime

import sqlalchemy
import pandas as pd


from pipelines.boliger import BoligaSold
from pipelines.boliger import Estate
from pipelines.boliger import Home
from pipelines.boliger import Nybolig


args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}

CONNECTION_URI = BaseHook.get_connection('bolig_db').get_uri()


def send_bolig(bolig, table, **kwargs):

    
    # postgres query roomSize will require "roomSize"
    bolig.columns = bolig.columns.str.lower()
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    bolig.to_sql(table, engine, if_exists='append')
    print(f'There were {len(bolig)} estates send to {table}')
    engine.dispose()

def bolig_from_home(**kwargs):
    # Home example
    api_name = 'home.dk'

    print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas\n')

    # instantiate a class
    homes = Home(url='https://home.dk/umbraco/backoffice/home-api/Search')

    # multipe pages per call
    params = dict(
        workers = 5,
        start_page = 10,
        end_page = 25,
        pagesize = 15,
        verbose = True,
    )

    params.update({}) # update from ui

    print(f'[+] Start {params["workers"]} threads for {params["pagesize"]} pagesize per call: '
          f'start at page {params["start_page"]} and at page {params["end_page"]} \n')
    homes.get_pages(**params)
    homes.store.drop(columns=['floorPlan', 'pictures'], inplace=True)

    send_bolig(homes.store, 'home')
    print(f'Data gathered {homes.store.shape[0]} rows\n')


def bolig_from_estate(**kwargs):
    # Estate example
    api_name = 'estate.dk'

    print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas\n')

    # instantiate a class
    estate = Estate(url='https://www.estate.dk/Services/PropertySearch/Search')

    # multipe pages per call
    params = dict(
        workers=5,
        start_page=10,
        end_page=25,
        pagesize=15,
        verbose=True,
    )

    params.update({})  # update from ui

    print(f'[+] Start {params["workers"]} threads for {params["pagesize"]} pagesize per call: '
          f'start at page {params["start_page"]} and at page {params["end_page"]} \n')
    estate.get_pages(**params)
    estate.store.drop(
        columns=['ImageReference', 'FloorPlanImageReference'], inplace=True)

    send_bolig(estate.store, 'estate')
    print(f'Data gathered {estate.store.shape[0]} rows\n')


def bolig_from_nybolig(**kwargs):
    # Nybolig Example
    api_name = 'nybolig.dk'

    print(f'\n[+] Using {api_name} to demostrate advance web scraping ideas\n')

    # instantiate a class
    nybolig = Nybolig(
        url='https://www.nybolig.dk/Services/PropertySearch/Search')

    # multipe pages per call
    params = dict(
        workers=5,
        start_page=10,
        end_page=25,
        pagesize=15,
        verbose=True,
    )

    params.update({})  # update from ui

    print(f'[+] Start {params["workers"]} threads for {params["pagesize"]} pagesize per call: '
          f'start at page {params["start_page"]} and at page {params["end_page"]} \n')
    nybolig.get_pages(**params)
    nybolig.store.drop(
        columns=['ImageReference', 'FloorPlanImageReference',  ], inplace=True)

    send_bolig(nybolig.store, 'nybolig')
    print(f'Data gathered {nybolig.store.shape[0]} rows\n')


with DAG(
    dag_id='example_from_home_estate_nybolig',
    description=f'Populate data from home.dk estate.dk and nybolig.dk',
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval='@once',
) as dag:

    push_home_data = PythonOperator(
        task_id='load_home_data',
        python_callable=bolig_from_home,
        op_kwargs={},
        dag=dag,
        provide_context=True

    )

    push_estate_data = PythonOperator(
        task_id='load_estate_data',
        python_callable=bolig_from_estate,
        op_kwargs={},
        dag=dag,
        provide_context=True

    )

    push_nybolig_data = PythonOperator(
        task_id='load_nybolig_data',
        python_callable=bolig_from_nybolig,
        op_kwargs={},
        dag=dag,
        provide_context=True

    )


push_home_data >> push_estate_data >> push_nybolig_data
