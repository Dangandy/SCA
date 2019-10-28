from mailchimp import MailChimp_ETL
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 1)
}

dag = DAG(dag_id="import_mailchimp", default_args=default_args, schedule_interval="@monthly")

def start():
    scrapper = MailChimp_ETL(
        os.environ.get('MC_API', ''),
        os.environ.get('MC_USER', ''), 
        os.environ.get('DB_URL', '')
        )
    extracted_data = scrapper.extract_data()
    transformed_data = scrapper.transform_data(extracted_data)
    scrapper.load_data(transformed_data,['campaigns','users'])
    scrapper.load_data(transformed_data,['lists','clicks','opens'])
    

exec_bookeo = PythonOperator(
    task_id="try_mailchimp",
    python_callable=start,
    dag=dag
)
