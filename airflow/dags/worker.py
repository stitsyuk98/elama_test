
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

import logging
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_CONN_ID = 'postgres'
FILE_TO_POSTGRES = ['transactions.csv', 'users.csv', 'webinar.csv']
DATABASE = 'postgres'
PUTH_TO_DATAFILE = '/usr/local/airflow/csv/'


def load_to_postgers(*args, **kwargs):
    files = FILE_TO_POSTGRES
    database = DATABASE

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=database)
    engine = pg.get_sqlalchemy_engine()

    for file in files:
        try:
            file_name = PUTH_TO_DATAFILE + file
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

def load_to_bigquery(*args, **kwargs):
    pass


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}
 
with DAG(
            'worker',
            default_args=default_args,
            schedule_interval=None,
            template_searchpath='/usr/local/airflow/sql',
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

    task_do_mat_view = PostgresOperator(
        task_id='do_mat_view',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sales_sumary.sql',
        dag=dag
    )

    task_load_to_bigquery = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
        dag=dag
    )

    task_load_to_postgers >> task_do_mat_view >> task_load_to_bigquery