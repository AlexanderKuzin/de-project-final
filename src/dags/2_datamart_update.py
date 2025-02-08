from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

import vertica_python
import pendulum
import logging
import contextlib

task_logger = logging.getLogger("airflow.task")

VERTICA_USER = Variable.get("vertica_user")
VERTICA_PASSWORD = Variable.get("vertica_password")

def load_to_vertica(date: str):

    vertica_conn = vertica_python.connect(
        host='vertica.tgcloudenv.ru',
        port=5433,
        user=VERTICA_USER,
        password=VERTICA_PASSWORD
    )
    
    sql_file = open("/sql/insert_into_global_metrics.sql", 'r')
    
    with contextlib.closing(vertica_conn.cursor()) as cur:
        query = f"""INSERT INTO STV2024080615__DWH.global_metrics (date_update,
                            currency_from,
                            amount_total,
                            cnt_transactions,
                            avg_transactions_per_account,
                            cnt_accounts_make_transactions) 
                            {sql_file.read().format(date)}"""
                            
        cur.execute(insert_expr)
        vertica_conn.commit() 

    vertica_conn.close()    

business_dt = '{{ ds }}'

with DAG(
        'stg_to_dwh_2_dag',
        schedule_interval='@daily', 
        start_date=pendulum.parse('2022-10-01'),
        max_active_runs = 1
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    load_stg_to_dwh = PythonOperator(
            task_id='load_stg_to_dwh',
            python_callable = load_to_vertica,
            op_kwargs={'date': business_dt})       

    (
            start >>
            load_stg_to_dwh >>
            end
    )