from datetime import datetime
from airflow.sdk import dag, task, Variable
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
import pandas as pd

@dag(
    dag_id="avg_coin_price",
    start_date=datetime(2026, 1, 26),
    schedule=None,
    catchup=False,
)
def avg_coin_price():

    @task
    def calculate_avg_price():
        """Initialize Spark, convert Pandas to Spark, and calculate average price"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import mean
        
        # Initialize Spark
        spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .appName("avg_coin_price") \
            .getOrCreate()
        
        print("🔄 Converting Pandas DataFrame to Spark DataFrame...")
        spark_df = spark.read.csv(
            "webhdfs://namenode:9870/raw_data/historical.csv",
            header=True,
            inferSchema=True
        )
        
        print(f"✅ Spark DataFrame created with {spark_df.count()} rows")
        spark_df.show(5)
        
        # Calculate average price
        avg_price = spark_df.select(mean("price")).collect()[0][0]
        print(f"📊 Average coin price: ${avg_price:.2f}")
        
        spark.stop()
        
        return float(avg_price)
    
    calculate_avg_price()

avg_coin_price()
