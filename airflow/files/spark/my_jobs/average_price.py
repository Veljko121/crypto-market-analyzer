from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
import sys
from datetime import datetime

def main():
    transformed_csv = sys.argv[1]
    mongo_host = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("avg_coin_price").config("spark.mongodb.write.connection.uri", mongo_host).getOrCreate()

    df = spark.read.csv(
        transformed_csv,
        header=True,
        inferSchema=True
    )

    avg_price = df.select(mean("price")).first()[0]

    avg_df = spark.createDataFrame([(float(avg_price), datetime.now())], ["average_price", "date_time"])

    avg_df.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", mongo_db) \
        .option("collection", "average_prices") \
        .save()

    spark.stop()
    
if __name__ == "__main__":
    main()
