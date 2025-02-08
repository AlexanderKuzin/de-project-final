from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable
from typing import List

import vertica_python
import boto3
import pendulum
import logging
import contextlib
import pandas as pd

task_logger = logging.getLogger("airflow.task")

AWS_ACCESS_KEY_ID = Variable.get("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = Variable.get("aws_secret_access_key")

VERTICA_USER = Variable.get("vertica_user")
VERTICA_PASSWORD = Variable.get("vertica_password")


def fetch_s3_file(key: str):
    filename = f'/data/{key}'
    
    # начинаем загрузку файла
    task_logger.info(f'Downloading file {key}.')
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='final-project',
        Key=key,
        Filename=filename
    )
    task_logger.info(f'Finished downloading.')
    
def load_dataset_file_to_vertica(
    key: str, 
    schema: str,
    table: str,
    columns: List[str],
    date: str
):
    
    filename = f'/data/{key}'
    df = pd.read_csv(filename)
    
    # выберем строки с датами равными дате текущего запуска дага для инкрементальной загрузки
    if key == 'currencies_history.csv':
        df_inc = df.loc[pd.to_datetime(df['date_update']).dt.strftime('%Y-%m-%d') == date].reset_index(drop=True)
    else:
        df_inc = df.loc[pd.to_datetime(df['transaction_dt']).dt.strftime('%Y-%m-%d') == date].reset_index(drop=True)
    
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    
    vertica_conn = vertica_python.connect(
        host='vertica.tgcloudenv.ru',
        port=5433,
        user=VERTICA_USER,
        password=VERTICA_PASSWORD
    )
       
    num_rows = len(df_inc)
    chunk_size = num_rows // 100
    
    # разбиваем датафрейм на чанки, загружаем в таблицу в vertica
    task_logger.info(f'Loading data for {date}')
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            task_logger.info(f"loading rows {start}-{end}")
            df_inc.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            task_logger.info("loaded")
            start += chunk_size + 1

    vertica_conn.close()

def fetch_s3_currencies(key: str, 
                        schema: str,
                        table: str,
                        columns: List[str],
                        date: str):
    fetch_s3_file(key)    
    load_dataset_file_to_vertica(key, schema, table, columns, date)
    
def fetch_s3_transactions(key: str, 
                        schema: str,
                        table: str,
                        columns: List[str],
                        date: str):
    for i in range(1,11):
        fetch_s3_file(f'{key}_{i}.csv')      
        load_dataset_file_to_vertica(f'{key}_{i}.csv', schema, table, columns, date)


business_dt = '{{ ds }}'

with DAG(
        '1_data_import',
        schedule_interval='@daily', 
        start_date=pendulum.parse('2022-10-01'),
        max_active_runs = 1
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    fetch_currencies = PythonOperator(
            task_id='fetch_currencies',
            python_callable=fetch_s3_currencies,
            op_kwargs=
                {'key': 'currencies_history.csv',
                'schema': 'STV2024080615__STAGING',
                'table': 'currencies',
                'columns': ['currency_code', 'currency_code_with', 'date_update', 'currency_with_div'],
                'date': business_dt               
                })
                
    fetch_transactions = PythonOperator(
            task_id='fetch_transactions',
            python_callable=fetch_s3_transactions,
            op_kwargs=
                {'key': 'transactions_batch',
                'schema': 'STV2024080615__STAGING',
                'table': 'transactions',
                'columns': ['operation_id', 'account_number_from', 'account_number_to', 'currency_code',
                            'country', 'status', 'transaction_type', 'amount', 'transaction_dt'],
                'date': business_dt               
                })        

    (
            start >>
            fetch_currencies >>
            fetch_transactions >>
            end
    )