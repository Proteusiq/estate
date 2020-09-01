
from collections import defaultdict
import json
import logging
import sys
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.dates import datetime
import pandas as pd
import sqlalchemy


CONNECTION_URI = BaseHook.get_connection('bolig_db').get_uri()
TABLE_NAME = 'postal_codes'

# fluentd + kibana
logging.basicConfig(stream=sys.stdout,level=logging.INFO,format="%(message)s")


args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}


# Re.write this using sqlsensor

def poke_query_postgres(query:str, **kwargs) -> pd.DataFrame:
    """Poke Database to see if the data query will succeed

    Keyword Arguments:
        query {str} -- run a query to onti postgress

    Returns:
        pd.DataFrame -- DataFrame of a given query

    Usage:
         
        >>> estates = query_postgres(query="SELECT * FROM estate LIMIT 1;")
    """
    ok = False
    if 'LIMIT' not in query:
        query = query.replace(';','')
        query += ' LIMIT 1;'
    
    
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    try:
        df = pd.read_sql(query, engine)
        ok = True

        logging.info(json.dumps({'query':query,'poke': ok}))

    except Exception as e:
        print(e.__dict__['statement'])
        logging.error(f'Ops no results match {query!r}', exc_info=True)
        logging.info(json.dumps({'query':query,'poke': ok}))
        return ok
    
    finally:
        engine.dispose()

    

    return ok

def postgres_kibana(query:str, **kwargs) -> None:
    """Get Database to see if the data query will succeed

    Keyword Arguments:
        query {str} -- run a query to onti postgress

    Returns:
        None -- DataFrame is sent to elasticsearch

    Usage:
         
        >>> postgres_elasticsearch(query="SELECT * FROM estate")
    """
    
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    
    try:
        df = pd.read_sql(query, engine)
        logging.info(df.to_json(orient='records', lines=True))
        

    except Exception as e:
        print(e.__dict__['statement'])
        logging.error(f'Ops postgress no results match {query!r}', exc_info=True)
        return f'Failed to send {query} query results to Kibana'
    finally:
        engine.dispose()

    return f'{query} query results was sent to Kibana'


    




with DAG(
    dag_id='send_data_to_kibana',
    description='Send Data to Kibana',
    default_args=args,
    # Start 10 minutes ago # days_ago(2)
    start_date=datetime.now(),
    schedule_interval='@once',
) as dag:

    poke_database = PythonOperator(
        task_id='poke_postgres',
        python_callable=poke_query_postgres,
        op_args=['SELECT * FROM boliga',],
        dag=dag,
        provide_context=True

    )

    send_data_kibana = PythonOperator(
        task_id='postgres_to_elasticsearch',
        op_args=['SELECT * FROM boliga',],
        dag=dag,
        python_callable=postgres_kibana,
        provide_context=True
    )


poke_database >> send_data_kibana


