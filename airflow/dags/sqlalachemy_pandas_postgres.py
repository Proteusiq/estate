import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago, timedelta, datetime

from sqlalchemy import create_engine
import pandas as pd


CONNECTION_URI = f"postgresql://{os.getenv('POSTGRES_USER','danpra')}:{os.getenv('POSTGRES_PASSWORD', 'postgrespwd')}@postgres:5432/bolig_db"
TABLE_NAME = 'boliga'

# time https://airflow.apache.org/docs/1.10.3/_modules/airflow/utils/dates.html
args = {
    'owner': 'Prayson',
    'catchup_by_default': False,
}



def load_data(**kwargs):
    """Print the Airflow context and kwargs variable from the context."""
    
    connection = create_engine(CONNECTION_URI)
    # this will come from ds

    df = pd.DataFrame({'house_id': [1, 2, 3],
                        'price': [11.1, 12.3, 14-5],
                        'typex': ['A', 'B', 'C'],
                        'timex': [datetime.now(), datetime.now(), datetime.now()]})
    df.to_sql(TABLE_NAME,
                connection,
                if_exists='append',
                )

    dt = pd.read_sql(f'SELECT * FROM {TABLE_NAME}', connection)


    connection.dispose()
    print(f'data loaded {len(df)} rows, total rows {len(dt)}')

    return len(dt)

def remove_data(**kwargs):

    connection = create_engine(CONNECTION_URI)
    ti = kwargs['ti']
    data_size = ti.xcom_pull(task_ids='load_data_with_sqlalchemy')
    
    print(f'current data: {data_size} rows')
    if data_size and data_size > 50:
        with connection.connect() as conn:
            conn.execute(f"DELETE FROM {TABLE_NAME};")

        print(f'data removed {repr(data_size)} rows')

    connection.dispose()
    return f'[+] data removing task completed'
    



with DAG(
    dag_id='df_to_postgres_sqlalchemy',
    description=f'Load data to postgress table {repr("boliga")}',
    default_args=args,
    start_date=datetime.now() - timedelta(minutes=10), # Start 10 minutes ago # days_ago(2)
    schedule_interval='*/10 * * * *',
    ) as dag:

    
    load_dataframe = PythonOperator(
        task_id='load_data_with_sqlalchemy',
        python_callable=load_data,
        dag=dag,
        provide_context=True

    )

    remove_dataframe = PythonOperator(
        task_id='remove_data_after_50_rows',
        dag=dag,
        python_callable=remove_data,
        provide_context=True
    )


    
load_dataframe >> remove_dataframe
