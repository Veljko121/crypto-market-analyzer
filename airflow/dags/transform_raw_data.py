from datetime import datetime

from airflow.sdk import dag, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="transform_raw_data",
    description="DAG in charge of curating raw data.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def transform_raw_data():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")

    prepare_initial_data = SparkSubmitOperator(
        task_id="prepare_initial_data",
        application="/opt/airflow/files/spark/transformation_jobs/prepare_initial_data.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        application_args=[
            f"{HDFS_DEFAULT_FS}/raw_data/packets_sample.jsonl",
            f"{HDFS_DEFAULT_FS}/transformed_data",
        ],
    )

    transform_packets_data = SparkSubmitOperator(
        task_id="transform_packets_data",
        application="/opt/airflow/files/spark/transformation_jobs/transform_packets_data.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
        ],
    )

    transform_ip_data = SparkSubmitOperator(
        task_id="transform_ip_data",
        application="/opt/airflow/files/spark/transformation_jobs/transform_ip_data.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
        ],
    )

    transform_transportation_data = SparkSubmitOperator(
        task_id="transform_transportation_data",
        application="/opt/airflow/files/spark/transformation_jobs/transform_transportation_data.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
        ],
    )

    transform_dns_queries_data = SparkSubmitOperator(
        task_id="transform_dns_queries_data",
        application="/opt/airflow/files/spark/transformation_jobs/transform_dns_queries_data.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
        ],
    )

    transform_dns_answers_data = SparkSubmitOperator(
        task_id="transform_dns_answers_data",
        application="/opt/airflow/files/spark/transformation_jobs/transform_dns_answers_data.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
        ],
    )

    (
        prepare_initial_data
        >> transform_packets_data
        >> transform_ip_data
        >> transform_transportation_data
        >> transform_dns_queries_data
        >> transform_dns_answers_data
    )


transform_raw_data()
