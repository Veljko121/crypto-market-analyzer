# curl -L -o ~/Downloads/coinmarketcap-historical-data.zip https://www.kaggle.com/api/v1/datasets/download/bizzyvinci/coinmarketcap-historical-data

from datetime import datetime
import os
from airflow.decorators import dag, task

@dag(
    dag_id="download_historical_data",
    description="This DAG downloads historical data from Kaggle",
    start_date=datetime(2026, 1, 26),
    catchup=False,
    schedule=None
)
def download_historical_data_dag():

    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    
    @task.bash
    def download_historical_data():
        return f"mkdir -p {airflow_home}/files/data && curl -L -o {airflow_home}/files/data/coinmarketcap-historical-data.zip https://www.kaggle.com/api/v1/datasets/download/bizzyvinci/coinmarketcap-historical-data"

    download_historical_data()

download_historical_data_dag()