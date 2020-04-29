import os

import psycopg2
import psycopg2.extras
import pandas as pd
 
 
class DF2Postgres:

    def __init__(self, df, table_name):
        self.df = df
        self.table_name = table_name
        

    def __enter__(self):
        self.connection = psycopg2.connect(user=os.getenv('POSTGRES_USER','danpra'),
                            password=os.getenv('POSTGRES_PASSWORD', 'postgrespwd'),
                            host='postgres',
                            port="5432",
                            database="airflow"
                            )
        self.connection.autocommit = True
                            
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.close()

    def send(self):

        mapper = {  'object': 'TEXT',
                    'int64': 'INT',
                    'float64': 'FLOAT',
                    'datetime64': 'DATETIME',
                    'bool': 'TINYINT',
                    'category': 'TEXT',
                    'timedelta[ns]': 'TEXT',
                    'datetime64[ns]': 'TEXT'
                }


        INNER_QUERY = ''
        for column in self.df.columns:
            INNER_QUERY += f',{column} {mapper[self.df[column].dtype.name]}'

        query = f"""
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                CREATE TABLE IF NOT EXISTS {self.table_name}(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4()
                {INNER_QUERY}
            );"""

        with self.connection.cursor() as c:
            
            c.execute(query)

            # from SO: https://stackoverflow.com/a/52124686/6858244
            # # df is the dataframe
            if len(self.df) > 0:
                df_columns = list(self.df)
                # create (col1,col2,...)
                columns = ",".join(df_columns)

                # create VALUES('%s', '%s",...) one '%s' per column
                values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

                #create INSERT INTO table (columns) VALUES('%s',...)
                insert_stmt = "INSERT INTO {} ({}) {}".format(self.table_name, columns, values)

                
                psycopg2.extras.execute_batch(c, insert_stmt, self.df.values)
            
        
