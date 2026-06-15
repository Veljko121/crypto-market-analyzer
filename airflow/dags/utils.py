import os
from airflow.sdk import Connection

HDFS_HOST = Connection.get("HDFS_CONNECTION").get_uri()
MONGO_HOST = os.getenv("AIRFLOW_CONN_MONGO_CONNECTION", "")

def spark_submit(job_path: str, master: Connection = Connection.get("SPARK_CONNECTION"), packages: list = [], args: list = []):
    airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    command = f"spark-submit --master {master.get_uri()}"
    if len(packages) > 0:
        command += " --packages " + ",".join(packages)
    command += f" {airflow_home}/files/spark/{job_path}"
    for arg in args:
        command += f" {arg}"
    return command

def transform_raw_csv_data() -> str:
    raw_data_folder = HDFS_HOST + "/raw_data"
    coins_csv = raw_data_folder + "/coins.csv"
    historical_csv = raw_data_folder + "/historical.csv"
    transformed_data_folder = HDFS_HOST + "/transformed_data/transformed_data_csv"
    spark_job_path = "my_jobs/reduce_raw_csv_data.py"
    command = spark_submit(spark_job_path, args=[coins_csv, historical_csv, transformed_data_folder])
    return command
