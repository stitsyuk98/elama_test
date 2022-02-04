
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from google.cloud import bigquery
from google.oauth2 import service_account

import os
import logging
import pandas as pd
from sqlalchemy import create_engine


POSTGRES_CONN_ID = 'postgres'
FILE_TO_POSTGRES = ['transactions.csv', 'users.csv', 'webinar.csv']
DATABASE = 'postgres'
PUTH_TO_DATAFILE = '/usr/local/airflow/csv/'
TAMPLATE_SEARCHPATH = '/usr/local/airflow/sql'
FILE_SQL_GET_SALES = TAMPLATE_SEARCHPATH + '/get_sales_sumary.sql'
CREDENTIONAL_PATH = '/usr/local/airflow/dags/keys/bidQueryPrivateKey.json'
TABLE_ID = 'amiable-hydra-340217.elama_test.sales_sumary'

def load_to_postgers(*args, **kwargs):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=DATABASE)
    engine = pg.get_sqlalchemy_engine()

    for file in FILE_TO_POSTGRES:
        try:
            file_name = PUTH_TO_DATAFILE + file
            table_name = file[:-4]
            for df in pd.read_csv(file_name, chunksize=1000, sep=','):
                df.to_sql(
                    table_name, 
                    engine,
                    index=False
                )
            logging.info(f'The file {file} uploaded successfully')
        except Exception as e:
            logging.error(f'The file {file} could not be uploaded\ndue to:\n{e}')


def load_to_bigquery(*args, **kwargs):
    if not os.path.isfile(FILE_SQL_GET_SALES) :
        logging.error(f'File {FILE_SQL_GET_SALES} not exists')
        return

    with open(FILE_SQL_GET_SALES, 'r') as file:
        script_select = file.read()

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=DATABASE)
    df = pg.get_pandas_df(script_select)
    logging.info(f'Data get successfully from file {FILE_SQL_GET_SALES}')
    
    credentials = service_account.Credentials.from_service_account_file(
            CREDENTIONAL_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    job = client.load_table_from_dataframe(df, TABLE_ID,)

    job.result()
    logging.info(f'Data load successfully to table {TABLE_ID}')


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}
 
with DAG(
            'worker',
            default_args=default_args,
            schedule_interval=None,
            template_searchpath=TAMPLATE_SEARCHPATH,
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