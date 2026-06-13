from airflow.decorators import dag, task
from airflow.sdk import dag, task
from utils import spark_submit, MONGO_HOST
import os

@dag(
    dag_id="current_price_vs_historical_price",
    schedule=None,
    catchup=False,
    tags=["streaming"],
)
def current_price_vs_historical_price():

    @task.bash(execution_timeout=None)
    def run_stream():
        kafka_host = "broker-1:9092,broker-2:9092"
        topic = "crypto_prices_live"
        mongo_host = MONGO_HOST
        mongo_db = "crypto"
        command = spark_submit(
            "my_jobs/current_price_vs_historical_price.py",
            packages=[
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
                "org.mongodb.spark:mongo-spark-connector_2.13:10.6.0"
            ],
            args=[kafka_host, topic, mongo_host, mongo_db]
        )
        return command

    run_stream()

current_price_vs_historical_price()
