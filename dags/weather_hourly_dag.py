
"""
Just putting a sample DAG docs here for now.
"""

from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import pendulum
import sys
import os

sys.path.insert(0, '/opt/airflow/etl')
from collect_current_weather import collect_hourly_weather


with DAG(
    'weather_hourly_collection',                    
    default_args = {
    'owner': 'airflow',                    
    'depends_on_past': False,              
    'email_on_failure': False,             
    'email_on_retry': False,
    'retries': 2,                          
    'retry_delay': timedelta(minutes=5),   
    },
    description='Collect current Manhattan weather data every hour',
    schedule_interval='@hourly',                           
    start_date=pendulum.today('America/New_York'),                     
    catchup=True,                                         
    tags=['weather', 'etl', 'hourly'],  
    max_active_runs=1,                 
) as dag:
    collect_weather_task = PythonOperator(
        task_id='collect_current_weather',           
        python_callable=collect_hourly_weather,                                    
    ) 

dag.doc_md = __doc__ 