import os

def spark_submit(job_path: str, master: str = os.getenv("AIRFLOW_CONN_SPARK_CONNECTION"), args: list = []):
    airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    command = f"spark-submit --master {master} {airflow_home}/files/spark/{job_path}"
    for arg in args:
        command += f" {arg}"
    return command
