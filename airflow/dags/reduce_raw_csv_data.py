from airflow.sdk import dag, task
from utils import spark_submit, HDFS_HOST

@dag(
    dag_id="reduce_raw_csv_data",
    schedule=None,
    catchup=False
)
def reduce_raw_csv_data():

    @task.bash
    def reduce():
        raw_data_folder = HDFS_HOST + "/raw_data"
        coins_csv = raw_data_folder + "/coins.csv"
        historical_csv = raw_data_folder + "/historical.csv"
        transformed_data_folder = HDFS_HOST + "/transformed_data/csv"
        return spark_submit("my_jobs/reduce_raw_csv_data.py", args=[coins_csv, historical_csv, transformed_data_folder])
    
    reduce()

reduce_raw_csv_data()
