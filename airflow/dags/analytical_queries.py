from datetime import datetime

from airflow.sdk import dag, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="analytical_queries",
    description="DAG in charge of running queries on top of transformed data.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
)
def analytical_queries():

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    MONGO_URI = Variable.get("MONGO_URI")

    packets_per_day = SparkSubmitOperator(
        task_id="packets_per_day",
        application="/opt/airflow/files/spark/analytical_jobs/packets_per_day.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/packets",
            MONGO_URI,
            "analytical",
        ],
    )

    top_source_ips = SparkSubmitOperator(
        task_id="top_source_ips",
        application="/opt/airflow/files/spark/analytical_jobs/top_source_ips.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/ip_flows",
            MONGO_URI,
            "analytical",
        ],
    )

    top_destination_ips = SparkSubmitOperator(
        task_id="top_destination_ips",
        application="/opt/airflow/files/spark/analytical_jobs/top_destination_ips.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/ip_flows",
            MONGO_URI,
            "analytical",
        ],
    )

    protocol_distribution = SparkSubmitOperator(
        task_id="protocol_distribution",
        application="/opt/airflow/files/spark/analytical_jobs/protocol_distribution.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/ip_flows",
            MONGO_URI,
            "analytical",
        ],
    )

    avg_packet_size = SparkSubmitOperator(
        task_id="avg_packet_size",
        application="/opt/airflow/files/spark/analytical_jobs/avg_packet_size.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/ip_flows",
            MONGO_URI,
            "analytical",
        ],
    )

    top_destination_ports = SparkSubmitOperator(
        task_id="top_destination_ports",
        application="/opt/airflow/files/spark/analytical_jobs/top_destination_ports.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/transport_flows",
            MONGO_URI,
            "analytical",
        ],
    )

    dns_query_frequency = SparkSubmitOperator(
        task_id="dns_query_frequency",
        application="/opt/airflow/files/spark/analytical_jobs/dns_query_frequency.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/dns_queries",
            MONGO_URI,
            "analytical",
        ],
    )

    dns_ttl_stability = SparkSubmitOperator(
        task_id="dns_ttl_stability",
        application="/opt/airflow/files/spark/analytical_jobs/dns_ttl_stability.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data/dns_answers",
            MONGO_URI,
            "analytical",
        ],
    )

    (
        packets_per_day
        >> top_source_ips
        >> top_destination_ips
        >> protocol_distribution
        >> avg_packet_size
        >> top_destination_ports
        >> dns_query_frequency
        >> dns_ttl_stability
    )


analytical_queries()
