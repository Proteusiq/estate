import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime

import sqlalchemy
import pandas as pd


from pipelines.boliga import BoligaRecent

CONNECTION_URI = (rf"postgresql://{os.getenv('POSTGRES_USER','danpra')}:"
                  rf"{os.getenv('POSTGRES_PASSWORD', 'postgrespwd')}@postgres:5432/bolig_db"
                  )
TABLE_NAME = f'recent_bolig_{Variable.get("postal","2650")}'

args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}


def get_bolig(postal, engine=None, **kwargs):
    
    bolig = BoligaRecent(url='https://api.boliga.dk/api/v2/search/results')

    bolig.get_pages(postal=postal, verbose=True)
    bolig.store.drop(columns=['images'], inplace=True)
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    bolig.store.to_sql(TABLE_NAME, engine, if_exists='append')
    engine.dispose()

    print(f'There were {len(bolig.store)} estates found in {postal}')
    

def process_completed(engine=None, **kwargs):
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    df = pd.read_sql(f'SELECT * FROM {TABLE_NAME}', engine)
    engine.dispose()
    print(f'Mining {TABLE_NAME} has {len(df)} rows at {datetime.now()}')
    return f'Data sending completed'
   


with DAG(
    dag_id=f'populate_{Variable.get("postal","2650")}_estates',
    description=f'Populate estates to {TABLE_NAME}',
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval='30 23 * * 1-5',
) as dag:

    push_bolig_postal = PythonOperator(
        task_id=f'load_{Variable.get("postal","2650")}_bolig_data',
        python_callable=get_bolig,
        op_args=[Variable.get("postal", "2650"),],
        dag=dag,
        provide_context=True
    )

   
    process_completed =  PythonOperator(
        task_id='mining_completed',
        dag=dag,
        python_callable=process_completed,
        provide_context=True
    )


push_bolig_postal >> process_completed
