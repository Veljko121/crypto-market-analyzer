from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("spark_connection_test").getOrCreate()
    spark.stop()

if __name__ == "__main__":
    main()
