import sys
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, lower, sum as spark_sum, lit, when, round as spark_round, greatest, count, row_number, coalesce
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
        .withColumn("mc", coalesce(col("price") * col("circulating_supply"), lit(0.0)))
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

    df_labeled = (
        df.
        join(df_top_coins.select("coin_id"), on="coin_id", how="left")
        .withColumn(
            "label",
            when(col("coin_id").isin([row["coin_id"] for row in df_top_coins.collect()]), col("name")).otherwise(lit("Others"))
        )
    )

    df_total_mc = (
        df
        .groupBy("date")
        .agg(spark_sum("mc").alias("total_mc"))
    )

    result = (
        df_labeled
        .groupBy("date", "label")
        .agg(spark_sum("mc").alias("mc"))
        .join(df_total_mc, on="date")
        .withColumn("Percentage", spark_round(col("mc") / col("total_mc") * 100, 4))
        .select("label", "date", "Percentage")
        .orderBy("date")
    )

    # Coalescing data with 0
    all_dates = result.select("date").distinct()
    all_labels = result.select("label").distinct()
    all_combinations = all_dates.crossJoin(all_labels)
    result = (
        all_combinations
        .join(result, on=["date", "label"], how="left")
        .withColumn("Percentage", coalesce(col("Percentage"), lit(0.0)))
        .orderBy("date")
    )

    # Downsampling
    total_dates = all_dates.count()
    step = max(int(total_dates / (max_data_points / (number_of_coins + 1))), 1)
    date_window = Window.orderBy("date")
    sampled_dates = (
        all_dates
        .withColumn("rn", row_number().over(date_window))
        .filter((col("rn") - 1) % step == 0)
        .select("date")
    )
    result_downsampled = result.join(sampled_dates, on="date")

    result_downsampled.write.format("mongodb").mode("overwrite").option("database", mongo_db).option("collection", "market_cap_share").save()

    spark.stop()
    

if __name__ == "__main__":
    main()
