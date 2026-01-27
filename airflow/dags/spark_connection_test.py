from datetime import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession

@dag(
    dag_id="spark_connection_test",
    start_date=datetime(2026, 1, 26),
    schedule=None,
    catchup=False,
)
def spark_connection_test():

    @task
    def test_connection():
        spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .appName("spark_connection_test") \
            .getOrCreate()
        
        print("✅ Spark task finished!")
        print(f"Spark version: {spark.version}")
        print(f"Master: {spark.sparkContext.master}")
        
        spark.stop()

    test_connection()

spark_connection_test()