import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from datetime import datetime, timezone

def main():
    csv_data_path = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    mongo_host = sys.argv[4]
    mongo_db = sys.argv[5]

    spark = SparkSession.builder.appName("market_movement").config("spark.mongodb.write.connection.uri", mongo_host).getOrCreate()

    df = spark.read.csv(csv_data_path, header=True, inferSchema=True)

    start_date_market_cap = df.filter(df["date"] == start_date) \
        .agg(sum(col("price") * col("circulating_supply")).alias("total_market_cap")) \
        .collect()[0]["total_market_cap"]

    end_date_market_cap = df.filter(df["date"] == end_date) \
        .agg(sum(col("price") * col("circulating_supply")).alias("total_market_cap")) \
        .collect()[0]["total_market_cap"]
    
    market_cap_difference = end_date_market_cap - start_date_market_cap
    market_cap_difference_percent_change = 100 * market_cap_difference / start_date_market_cap
    result = spark.createDataFrame([{
        "date_calculated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": start_date,
        "start_date_market_cap": start_date_market_cap,
        "end_date": end_date,
        "end_date_market_cap": end_date_market_cap,
        "market_cap_change": market_cap_difference,
        "market_cap_change_percent": market_cap_difference_percent_change,
    }])

    result.write.format("mongodb") \
        .mode("append") \
        .option("database", mongo_db) \
        .option("collection", "market_movement") \
        .save()
    
    spark.stop()

if __name__ == "__main__":
    main()
