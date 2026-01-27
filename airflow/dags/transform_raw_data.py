from airflow.sdk import dag, task
from utils import spark_submit, get_hdfs_host

@dag(
    dag_id="transform_raw_data"
)
def transform_raw_data():

    @task.bash
    def transform_data():
        hdfs_host  = get_hdfs_host()
        raw_data_folder = hdfs_host + "/raw_data"
        coins_csv = raw_data_folder + "/coins.csv"
        historical_csv = raw_data_folder + "/historical.csv"
        transformed_data_folder = hdfs_host + "/transformed_data"
        return spark_submit("my_jobs/transform_data.py", args=[coins_csv, historical_csv, transformed_data_folder])

    transform_data()

transform_raw_data()
