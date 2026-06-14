from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, first, last, round as spark_round, min as spark_min, max as spark_max
import sys

def main():
    kafka_host = sys.argv[1]
    topic = sys.argv[2]
    mongo_host = sys.argv[3]
    mongo_db = sys.argv[4]

    spark = SparkSession.builder.appName("price_change").config("spark.sql.streaming.metricsEnabled", "false").getOrCreate()

    schema = "timestamp STRING, id STRING, name STRING, current_price DOUBLE, circulating_supply DOUBLE, total_volume DOUBLE"
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_host)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()

        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    )

    result_df = (
        df
        .withWatermark("timestamp", "1 minute")
        .groupBy(
            "id",
            "name",
            window("timestamp", "5 minutes", "1 minute")
        )
        .agg(
            spark_min("current_price").alias("min_price"),
            spark_max("current_price").alias("max_price"),
            spark_max("circulating_supply").alias("circulating_supply")
        )
        .withColumn(
            "price_change_pct",
            spark_round(
                (col("max_price") - col("min_price")) / col("min_price") * 100,
                3
            )
        )
        .withColumn(
            "market_cap",
            col("max_price") * col("circulating_supply")
        )
        .select(
            col("id"),
            col("name"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("min_price"),
            col("max_price"),
            col("price_change_pct"),
            col("market_cap"),
        )
    )

    query = (
        result_df.writeStream
        .format("mongodb")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoint/price_change")
        .option("forceDeleteTempCheckpointLocation", "true")
        .option("spark.mongodb.connection.uri", mongo_host)
        .option("spark.mongodb.database", mongo_db)
        .option("spark.mongodb.collection", "price_change")
        .start()
    )

    query.awaitTermination()

    spark.stop()

if __name__ == "__main__":
    main()
