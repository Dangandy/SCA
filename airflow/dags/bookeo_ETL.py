from bookeo import Bookeo_ETL
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 18)
}

dag = DAG(dag_id="import_bookeo", default_args=default_args, schedule_interval="@weekly")

def start():
    sca_data = Bookeo_ETL(
        os.environ.get('SECRET_KEY', ''),
        os.environ.get('CASA_LOMA', ''),
        os.environ.get('BLACK_CREEK', ''),
        os.environ.get('DB_URL', '')
    )
    raw_data = sca_data.extract_data()
    sca_dfs = sca_data.transform_data(raw_data)
    sca_data.load_data(sca_dfs)

exec_bookeo = PythonOperator(
    task_id="try_bookeo",
    python_callable=start,
    dag=dag
)
