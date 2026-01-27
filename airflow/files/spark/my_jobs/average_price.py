from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
import sys
from datetime import datetime
from pyspark.sql.functions import lit

def main():
    transformed_parquet = sys.argv[1]
    mongo_host = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("avg_coin_price").config("spark.mongodb.write.connection.uri", mongo_host).getOrCreate()

    df = spark.read.parquet(transformed_parquet)

    avg_price_df = df.groupBy("id", "name").agg(mean("price").alias("average_price"))
    avg_price_df = avg_price_df.withColumn("date_time", lit(datetime.now()))

    avg_price_df.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", mongo_db) \
        .option("collection", "average_prices") \
        .save()

    spark.stop()
    
if __name__ == "__main__":
    main()
