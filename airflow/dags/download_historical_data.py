from datetime import datetime
import os
from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
import urllib.request
import zipfile

@dag(
    dag_id="download_historical_data",
    description="This DAG downloads historical data from Kaggle",
    start_date=datetime(2026, 1, 26),
    catchup=False,
    schedule=None
)
def download_historical_data_dag():

    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    
    @task
    def download_data():
        data_path = os.path.join(airflow_home, "files", "data", "coinmarketcap-historical-data.zip")
        data_link = "https://www.kaggle.com/api/v1/datasets/download/bizzyvinci/coinmarketcap-historical-data"
        urllib.request.urlretrieve(data_link, data_path)
        return data_path

    @task
    def unzip_historical_data(data_local_path: str):
        extracted_data_path = f"{airflow_home}/files/data/unzipped/"
        with zipfile.ZipFile(data_local_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_data_path)
        return extracted_data_path

    @task
    def load_data(local_path: str):
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        data_hdfs_path = "/raw_data/"
        hdfs_hook.load_file(
            source=local_path, destination=data_hdfs_path, overwrite=True
        )
        return data_hdfs_path

    @task
    def check_hdfs_file(path):
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        if hdfs_hook.check_for_path(hdfs_path=path):
            print(f"File {path} found in HDFS.")
        else:
            print(f"File {path} not found.")

    data_local_path = download_data()
    extracted_data_path = unzip_historical_data(data_local_path)
    data_hdfs_path = load_data(extracted_data_path)
    check_hdfs_file(data_hdfs_path)


download_historical_data_dag()
