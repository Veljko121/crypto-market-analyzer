import os
from airflow.sdk import Variable, Connection

def spark_submit(job_path: str, master: Connection = Connection.get("SPARK_CONNECTION"), args: list = []):
    airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    command = f"spark-submit --master {master.get_uri()} {airflow_home}/files/spark/{job_path}"
    for arg in args:
        command += f" {arg}"
    return command

def get_hdfs_host():
    return Connection.get("HDFS_CONNECTION").get_uri()
