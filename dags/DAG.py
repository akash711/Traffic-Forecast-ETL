from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import datetime
from datetime import timedelta
from utils import create_db, insert_into_weather_db, insert_into_traffic_db
from airflow.models import Variable
import os



default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime.today(),
    "email": ["my_email@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval":timedelta(hours=12)
}

with DAG('traffic_prediction_etl', default_args=default_args) as dag:
    

    create_tables = PythonOperator(
        task_id = 'create_tables'
        python_callable = create_db
        dag = dag
        )

    populate_weather_db = PythonOperator(
        task_id = 'populate_weather_db'
        python_callable = insert_into_weather_db
        dag = dag
        )

    populate_traffic_db = PythonOperator(
        task_id = 'populate_traffic_db'
        python_callable = insert_into_traffic_db
        dag = dag
        )

create_tables >> populate_weather_db >> populate_traffic_db