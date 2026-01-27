from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
import os

@dag(
    dag_id="load_raw_data",
    description="This DAG loads raw data to HDFS.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def load_raw_data():

    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

    @task
    def unzip_historical_data():
        import zipfile
        zip_path = f"{airflow_home}/files/data/coinmarketcap-historical-data.zip"
        extract_path = f"{airflow_home}/files/data/unzipped/"
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        return extract_path

    @task
    def load_data(local_path: str):
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        hdfs_destination = "/raw_data/"
        hdfs_hook.load_file(
            source=local_path, destination=hdfs_destination, overwrite=True
        )
        return hdfs_destination

    @task
    def check_hdfs_file(path):
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        if hdfs_hook.check_for_path(hdfs_path=path):
            print(f"File {path} found in HDFS.")
        else:
            print(f"File {path} not found.")

    unzipped_files_path = unzip_historical_data()
    hdfs_file = load_data(unzipped_files_path)
    check_hdfs_file(hdfs_file)


load_raw_data()
