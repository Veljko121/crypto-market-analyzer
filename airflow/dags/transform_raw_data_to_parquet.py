from airflow.sdk import dag, task
from utils import spark_submit, HDFS_HOST

@dag(
    dag_id="transform_raw_data_to_parquet"
)
def transform_raw_data_to_parquet():

    @task.bash
    def transform():
        raw_data_folder = HDFS_HOST + "/raw_data"
        coins_csv = raw_data_folder + "/coins.csv"
        historical_csv = raw_data_folder + "/historical.csv"
        transformed_data_folder = HDFS_HOST + "/transformed_data/parquet"
        return spark_submit("my_jobs/transform_csv_to_parquet.py", args=[coins_csv, historical_csv, transformed_data_folder])

    transform()

transform_raw_data_to_parquet()
