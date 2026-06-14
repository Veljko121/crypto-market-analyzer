from airflow.sdk import dag, task
from utils import spark_submit, MONGO_HOST

@dag(
    dag_id="price_deviation_last_5_min.py",
    schedule=None,
    catchup=False,
    tags=["streaming"],
)
def price_deviation_last_5_min():

    @task.bash(execution_timeout=None)
    def calculate_price_deviation_last_5_min():
        kafka_host = "broker-1:9092,broker-2:9092"
        topic = "crypto_prices_live"
        mongo_host = MONGO_HOST
        mongo_db = "crypto"
        command = spark_submit(
            "my_jobs/price_deviation_last_5_min.py",
            packages=[
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
                "org.mongodb.spark:mongo-spark-connector_2.13:10.6.0"
            ],
            args=[kafka_host, topic, mongo_host, mongo_db]
        )
        return command

    calculate_price_deviation_last_5_min()

price_deviation_last_5_min()
