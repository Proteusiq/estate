from collections import defaultdict
import os

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime

import sqlalchemy  
import pandas as pd
import requests


CONNECTION_URI = BaseHook.get_connection('bolig_db').get_uri()
TABLE_NAME = 'postal_codes'

# time https://airflow.apache.org/docs/1.10.3/_modules/airflow/utils/dates.html
args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}


def get_postal(only_postal:bool=True, **kwargs) -> pd.DataFrame:
    '''
    Simple function that returns DK postal codes for DAWA. If only_post is false,
    postal code name, kommune, latitude and longitude are returned
    
    how to use:
    df = get_postal(only_postal=False)
    '''

    post_data = defaultdict(list)
    uri = 'http://dawa.aws.dk/postnumre'

    with requests.Session() as httpx:
        r = httpx.get(uri)

    assert r.ok, 'Connection Error'

    posts = r.json()

    for _, post in enumerate(posts):
        post_data['Postal'].append(post['nr'])
        post_data['Name'].append(post['navn'])
        post_data['Kommuner'].append(
            post['kommuner'][0]['navn'] if post['kommuner'] else None)
        post_data['Longitude'].append(post['visueltcenter'][0])
        post_data['Latitude'].append(post['visueltcenter'][1])
        # post_data['bbox'].append(post['bbox'])

    if only_postal:
        df = pd.DataFrame(post_data)[['Postal']]
    else:
        df = pd.DataFrame(post_data)

    df.columns = df.columns.str.lower()
    df['postal'] = df['postal'].astype(int)

    engine = sqlalchemy.create_engine(CONNECTION_URI)
    df.to_sql(TABLE_NAME, engine, if_exists='replace')
    engine.dispose()

    return f'postal data pushed to {TABLE_NAME}'

def check_postal(**kwargs):

    engine = sqlalchemy.create_engine(CONNECTION_URI)

    print(f'checking if postal data is in {TABLE_NAME}')
    
    try:
        df = pd.read_sql(f'SELECT * FROM {TABLE_NAME}_no', engine)

        print(f'postal data with {df.shape} exits')
        engine.dispose()
    except sqlalchemy.exc.ProgrammingError as e:
        print(e.__dict__['statement'])
        return False

    return True 



with DAG(
    dag_id='populate_postal_codes',
    description=f'Populate postal code to {TABLE_NAME}',
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval='@once',
) as dag:

    push_postal_data = PythonOperator(
        task_id='load_postal_data',
        python_callable=get_postal,
        op_args=[False,],
        dag=dag,
        provide_context=True

    )

    check_postal_data = PythonOperator(
        task_id='check_postal_data',
        dag=dag,
        python_callable=check_postal,
        provide_context=True
    )


push_postal_data >> check_postal_data
