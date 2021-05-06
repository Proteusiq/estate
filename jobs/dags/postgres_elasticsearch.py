
from collections import defaultdict
import json
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime
import pandas as pd
import requests
import sqlalchemy


CONNECTION_URI = BaseHook.get_connection('bolig_db').get_uri()
TABLE_NAME = 'postal_codes'


args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}


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

    except sqlalchemy.exc.ProgrammingError as e:
        print(e.__dict__['statement'])
    
    finally:
        engine.dispose()

    return ok

def postgres_elasticsearch(query:str, **kwargs) -> None:
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

        
        df['_id'] = df['guid']

        json_data = ''
        for json_document in df.to_json(orient='records', lines=True).split('\n'):
            json_dict = json.loads(json_document)
            metadata = json.dumps({'index': {'_id': json_dict['_id']}})
            json_dict.pop('_id')
            json_data += metadata + '\n' + json.dumps(json_dict) + '\n'

        headers = {'Content-type': 'application/json', 
                   'Accept': 'text/plain'}
        URI = 'http://elasticsearch:9200/estates/prices/_bulk'
        with requests.Session() as httpx:
            r = httpx.post(URI, data=json_data, headers=headers, timeout=60)
        
        print(f'Post Status {r.status_code} and reasons: {r.reason}')         

       

    except sqlalchemy.exc.ProgrammingError as e:
        print(e.__dict__['statement'])
    
    finally:
        engine.dispose()


    return f'postal data pushed to {TABLE_NAME}'




with DAG(
    dag_id='send_data_to_elastic',
    description='Send Data to Elasticsearch',
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

    send_data_elasticsearch = PythonOperator(
        task_id='postgres_to_elasticsearch',
        op_args=['SELECT * FROM boliga',],
        dag=dag,
        python_callable=postgres_elasticsearch,
        provide_context=True
    )


poke_database >> send_data_elasticsearch


