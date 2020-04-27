import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago, timedelta, datetime

import psycopg2
import psycopg2.extras
import pandas as pd

from utils.df_to_pandas import DF2Postgres

# time https://airflow.apache.org/docs/1.10.3/_modules/airflow/utils/dates.html
args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}

dag = DAG(
    dag_id='dataframe_to_postgres',
    description=f'Load data to postgress table {repr("boliga")}',
    default_args=args,
    start_date=datetime.now() - timedelta(minutes=10), # Start 10 minutes ago # days_ago(2)
    schedule_interval='*/2 * * * *',
)


def load_data(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print(f'kwargs = {kwargs}')
    print(f'ds = {ds}')
    
    # this will come from ds
    TABLE_NAME = 'boliga'
    df = pd.DataFrame({'house_id': [1, 2, 3], 
                        'price': [11.1, 12.3, 14-5],
                        'typex': ['A', 'B', 'C'],
                        'timex': [datetime.now(), datetime.now(), datetime.now()] })


    with DF2Postgres(df=df, table_name=TABLE_NAME) as d:
        d.send()
   
    return f'data loaded {len(df)} rows'


load_dataframe = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data,
    dag=dag,
    op_args=['table name?']
)

load_dataframe
