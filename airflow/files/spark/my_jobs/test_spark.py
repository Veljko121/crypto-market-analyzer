from pyspark.sql import SparkSession

def run_spark_job():
    """Your Spark job logic"""
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("spark_test_job") \
        .getOrCreate()
    
    print("✅ Spark task started!")
    print(f"Spark version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    
    # Your actual Spark work here
    df = spark.range(100)
    df.show()
    
    result = df.count()
    print(f"Result: {result}")
    
    spark.stop()
    return result

if __name__ == "__main__":
    run_spark_job()