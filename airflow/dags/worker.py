
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import logging
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_CONN_ID = 'postgres'
FILE_TO_POSTGRES = ['transactions.csv', 'users.csv', 'webinar.csv']
DATABASE = 'postgres'



def load_to_postgers(*args, **kwargs):
    files = FILE_TO_POSTGRES
    database = DATABASE

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=database)
    engine = pg.get_sqlalchemy_engine()

    for file in files:
        try:
            file_name = '/usr/local/airflow/sql/' + file
            table_name = file[:-4]
            for df in pd.read_csv(file_name, chunksize=1000, sep=','):
                df.to_sql(
                    table_name, 
                    engine,
                    index=False,
                    if_exists='append' # if the table already exists, append this data
                )
            logging.info(f'The file {file} uploaded successfully')
        except Exception as e:
            logging.error(f'The file {file} could not be uploaded\ndue to:\n{e}')




default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    # 'template_searchpath': '/usr/local/airflow/sql',
}

with DAG(
            'worker',
            default_args=default_args,
            schedule_interval=None,
            # params={
            #     'file_to_postgers': ['transactions.csv', 'users.csv', 'webinar.csv'],
            #     'database': 'postgres'
            # }
        ) as dag:
    
    task_load_to_postgers = PythonOperator(
        task_id='load_to_postgers',
        python_callable=load_to_postgers,
        dag=dag
    )