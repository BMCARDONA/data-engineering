"""
DAG to collect hourly weather data for Manhattan neighborhoods.
"""

from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
import sys
import os

sys.path.insert(0, '/opt/airflow/etl')
from collect_current_weather import collect_hourly_weather

@dag(
    dag_id='weather_hourly_collection',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Collect current Manhattan weather data every hour',
    schedule='@hourly',
    start_date=pendulum.datetime(2026, 1, 10, tz='America/New_York'),
    catchup=False,  
    tags=['weather', 'etl', 'hourly'],
    max_active_runs=1,
)
def weather_hourly_collection():
    @task()
    def collect_current_weather():
        collect_hourly_weather()
    collect_current_weather()

weather_dag = weather_hourly_collection()
