import sys
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, lower, sum as spark_sum, coalesce, lit, row_number, rank
from pyspark.sql.window import Window

def main():
    historical_data_path = sys.argv[1]
    coins_data_path = sys.argv[2]
    start_date = sys.argv[3]
    end_date = sys.argv[4]
    number_of_coins = int(sys.argv[5])
    mongo_host = sys.argv[6]
    mongo_db = sys.argv[7]

    max_data_points = 2000

    spark = SparkSession.builder.appName("coin_race").config("spark.mongodb.write.connection.uri", mongo_host).getOrCreate()

    historical_df = spark.read.csv(historical_data_path, inferSchema=True, header=True)
    coins_df = spark.read.csv(coins_data_path, inferSchema=True, header=True)

    # Historical + coins
    df = (
        historical_df
        .join(
            coins_df, on=historical_df.coin_id == coins_df.id, how="inner"
        )
        .filter(
            (col("date") >= start_date) & (col("date") <= end_date) &
            (~lower(col("name")).like("%innovative%"))
        )
        .withColumn("mc", coalesce(col("price") * col("circulating_supply"), lit(0.0)))
    )

    # Find top coins
    df_top_coins = (
        df
        .filter(col("date") == start_date)
        .orderBy(col("mc").desc())
        .limit(number_of_coins)
        .select("coin_id", "name")
    )

    df_top_coins = (
        df
        .join(df_top_coins.select("coin_id"), on="coin_id", how="inner")
    )

    rank_window = Window.partitionBy("date").orderBy(col("mc").desc())

    result = (
        df_top_coins
        .withColumn("rank", rank().over(rank_window))
        .select(
            col("date"),
            col("name"),
            col("mc"),
            col("rank")
        )
        .orderBy("date", "rank")
    )

    # Downsampling
    all_dates = result.select("date").distinct()
    total_dates = all_dates.count()
    step = max(int(total_dates / (max_data_points / number_of_coins)), 1)
    date_window = Window.orderBy("date")
    sampled_dates = (
        all_dates
        .withColumn("rn", row_number().over(date_window))
        .filter((col("rn") - 1) % step == 0)
        .select("date")
    )
    result_downsampled = result.join(sampled_dates, on="date")

    result_downsampled.write.format("mongodb").mode("overwrite").option("database", mongo_db).option("collection", "coin_race").save()

    spark.stop()

if __name__ == "__main__":
    main()
