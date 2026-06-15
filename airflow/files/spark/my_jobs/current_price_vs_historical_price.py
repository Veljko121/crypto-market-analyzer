from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round as spark_round, lower
import sys


def main():
    kafka_host = sys.argv[1]
    topic = sys.argv[2]
    mongo_host = sys.argv[3]
    mongo_db = sys.argv[4]

    spark = SparkSession.builder.appName("stream_historical_join").config("spark.mongodb.read.connection.uri", mongo_host).getOrCreate()

    schema = "id STRING, name STRING, current_price DOUBLE, circulating_supply DOUBLE, total_volume DOUBLE"
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_host)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()

        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

    historical_avg_df = (
        spark.read
        .format("mongodb")
        .option("spark.mongodb.read.connection.uri", mongo_host)
        .option("database", mongo_db)
        .option("collection", "average_prices")
        .load()

        .select(
            "name",
            col("average_price").alias("historical_avg_price")
        )
    )

    # lower(historical_name) == lower(real_time_id)
    joined_df = (
        stream_df
        .join(
            historical_avg_df,
            on=lower(stream_df["id"]) == lower(historical_avg_df["name"]),
            how="inner"
        )
    )

    result_df = (    
        joined_df
        .withColumn(
            "deviation_pct",
            spark_round((col("current_price") - col("historical_avg_price")) / col("historical_avg_price") * 100, 2).cast("double")
        )
        .filter(col("historical_avg_price").isNotNull())
        .withColumn("market_cap", col("current_price") * col("circulating_supply"))
    )

    query = (
        result_df
        .drop(historical_avg_df["name"])
        .writeStream
        .format("mongodb")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoint/stream_historical_join")
        .option("forceDeleteTempCheckpointLocation", "true")
        .option("spark.mongodb.connection.uri", mongo_host)
        .option("spark.mongodb.database", mongo_db)
        .option("spark.mongodb.collection", "stream_historical_join")
        .start()
    )

    query.awaitTermination()

    spark.stop()

if __name__ == "__main__":
    main()
