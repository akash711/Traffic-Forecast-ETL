from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import datetime
from datetime import timedelta
from utils import create_db, insert_into_weather_db, insert_into_traffic_db
from airflow.models import Variable


config_path = Variable.get("config_path")

default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021,1,25),
    "email": ["my_email@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG('traffic', template_searchpath = config_path, default_args=default_args, schedule_interval=timedelta(hours=12)) as dag:
    

	create_tables = PythonOperator(
	        task_id = 'create_tables',
	        python_callable = create_db,
	        dag = dag
	        )


    
	populate_weather_db = PythonOperator(
        task_id = 'populate_weather_db',
        python_callable = insert_into_weather_db,
        dag = dag
        )

	populate_traffic_db = PythonOperator(
        task_id = 'populate_traffic_db',
        python_callable = insert_into_traffic_db,
        dag = dag
        )

create_tables >> populate_weather_db >> populate_traffic_db
