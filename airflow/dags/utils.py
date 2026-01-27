import os
from airflow.sdk import Connection

def spark_submit(job_path: str, master: Connection = Connection.get("SPARK_CONNECTION"), packages: list = [], args: list = []):
    airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    command = f"spark-submit --master {master.get_uri()}"
    if len(packages) > 0:
        command += " --packages " + ",".join(packages)
    command += f" {airflow_home}/files/spark/{job_path}"
    for arg in args:
        command += f" {arg}"
    return command

def get_hdfs_host():
    return Connection.get("HDFS_CONNECTION").get_uri()

def get_mongo_host():
    return os.getenv("AIRFLOW_CONN_MONGO_CONNECTION")
