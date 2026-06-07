from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import col, lower, sum as spark_sum, count, round as spark_round, row_number, greatest, lit
from pyspark.sql.window import Window
from datetime import datetime

def main():
    historical_data_path = sys.argv[1]
    coins_data_path = sys.argv[2]
    start_date = sys.argv[3]
    end_date = sys.argv[4]
    number_of_coins = int(sys.argv[5])
    mongo_host = sys.argv[6]
    mongo_db = sys.argv[7]

    max_data_points = 2000
    points_per_coin = max_data_points / number_of_coins
    total_days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days

    spark = SparkSession.builder.appName("top_coins_trends").config("spark.mongodb.write.connection.uri", mongo_host).getOrCreate()

    historical_df = spark.read.csv(historical_data_path, inferSchema=True, header=True)
    coins_df = spark.read.csv(coins_data_path, inferSchema=True, header=True)

    df = (
        historical_df
        .join(
            coins_df, on=historical_df.coin_id == coins_df.id, how="inner"
        )
        .filter(
            (col("date") >= start_date) & (col("date") <= end_date) &
            (~lower(col("name")).like("%innovative%"))
        )
        .withColumn("mc", col("price") * col("circulating_supply"))
    )

    df_top_coins = (
        df
        .groupBy(
            "coin_id", "name"
        )
        .agg(
            spark_sum("mc").alias("mc_summed")
        )
        .orderBy(
            col("mc_summed").desc()
        )
        .limit(number_of_coins)
    )

    result = (
        df
        .join(
            df_top_coins.select("coin_id"), on="coin_id"
        )
        .select(
            "name", "date", "mc"
        )
        .orderBy(
            "date"
        )
    )

    # Downsampling
    coin_days_df = result.groupBy("name").agg(count("date").alias("coin_days"))
    coin_step_df = coin_days_df.withColumn(
        "step",
        greatest(
            spark_round(
                col("coin_days") / spark_round(points_per_coin * col("coin_days") / total_days, 0)
            ).cast("int"),
            lit(1)
        )
    )
    row_window = Window.partitionBy("name").orderBy("date")
    result_downsampled = (
        result
        .withColumn("rn", row_number().over(row_window))
        .join(coin_step_df.select("name", "step"), on="name")
        .filter((col("rn") - 1) % col("step") == 0)
        .drop("rn", "step")
    )

    # Writing results
    result_downsampled.write.format("mongodb").mode("overwrite").option("database", mongo_db).option("collection", "top_coins_trends").save()

    spark.stop()


if __name__ == "__main__":
    main()
