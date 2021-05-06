"""
[Deprecated]: Use pipelines.boliger.boliga
This script contains an example of self generating task from UI Variable
input of single zipcode

Contains two major codes: thread and unnessary data copy: bug > pd.DataFrame is not thread safe, flow: pd.append used in loop
"""

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime
import sqlalchemy
import pandas as pd
from pipelines.boligax import BoligaRecent


CONNECTION_URI = BaseHook.get_connection('bolig_db').get_uri()
TABLE_NAME = f'recent_bolig_{Variable.get("postal",2650)}'

args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}


def get_bolig(postal:int, engine:sqlalchemy.types.TypeEngine=None, **kwargs) -> None:
    """get bolig[estate] from a given postal code

    Arguments:
        postal {int} -- Danish postal code: e.g. 2560

    Keyword Arguments:
        engine {sqlalchemy.types.TypeEngine} -- Connection Engine to Database (default: {None})
    """
    
    bolig = BoligaRecent(url='https://api.boliga.dk/api/v2/search/results')

    bolig.get_pages(postal=postal, verbose=True)

    if not bolig.store.empty:
    
        # columns with dict causes issues. stringfy thme
        columns = bolig.store.select_dtypes('object').columns
        bolig.store[columns] = bolig.store[columns].astype(str)

        engine = sqlalchemy.create_engine(CONNECTION_URI)
        bolig.store.to_sql(TABLE_NAME, engine, if_exists='append')
        engine.dispose()

        print(f'There were {len(bolig.store)} estates found in {postal}')
    else:
        print(f'There were no estates found in {postal}')
        

def process_completed(engine:sqlalchemy.types.TypeEngine = None, **kwargs) -> None:
    """checks if data populating was complete

    Keyword Arguments:
        engine {sqlalchemy.types.TypeEngine} -- Connection Engine to Database (default: {None})
    """
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    df = pd.read_sql(f'SELECT * FROM {TABLE_NAME}', engine)
    engine.dispose()
    print(f'Mining {TABLE_NAME} has {len(df)} rows at {datetime.now()}')
    return f'Data sending completed'
   


with DAG(
    dag_id=f'populate_{Variable.get("postal",2650)}_estates',
    description=f'Populate estates to {TABLE_NAME}',
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval='30 23 * * 1-5',
) as dag:

    push_bolig_postal = PythonOperator(
        task_id=f'load_{Variable.get("postal",2650)}_bolig_data',
        python_callable=get_bolig,
        op_args=[Variable.get("postal", 2650),],
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
