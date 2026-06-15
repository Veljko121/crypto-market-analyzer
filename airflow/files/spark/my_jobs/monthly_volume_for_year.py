import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, sum as spark_sum, avg, month, coalesce, lit, round as spark_round
from pyspark.sql.window import Window

def main():
    historical_data_path = sys.argv[1]
    coins_data_path = sys.argv[2]
    year = int(sys.argv[3])
    mongo_host = sys.argv[4]
    mongo_db = sys.argv[5]

    spark = SparkSession.builder.appName("monthly_volume_for_year").config("spark.mongodb.write.connection.uri", mongo_host).getOrCreate()

    historical_df = spark.read.csv(historical_data_path, inferSchema=True, header=True)
    coins_df = spark.read.csv(coins_data_path, inferSchema=True, header=True)

    df = (
        historical_df
        .join(
            coins_df, on=historical_df.coin_id == coins_df.id, how="inner"
        )
        .filter(
            (col("date").startswith(str(year))) &
            (~lower(col("name")).like("%innovative%"))
        )
        .withColumn("volume", coalesce(col("volume_24h"), lit(0.0)))
    )

    # Prosečni dnevni volumen po mesecu (suma svih coina po danu, pa prosek po mesecu)
    df_daily_total = (
        df
        .groupBy("date")
        .agg(spark_sum("volume").alias("daily_total_volume"))
        .withColumn("month", month(col("date")))
    )

    df_volume_monthly = (
        df_daily_total
        .groupBy("month")
        .agg(spark_round(spark_sum("daily_total_volume"), 2).alias("monthly_volume"))
        .orderBy("month")
    )

    # Godišnji prosek kao window funkcija
    yearly_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    result = (
        df_volume_monthly
        .withColumn("yearly_avg", spark_round(avg("monthly_volume").over(yearly_window), 2))
    )

    result.write.format("mongodb").mode("overwrite").option("database", mongo_db).option("collection", "monthly_volume_for_year").save()

    spark.stop()

if __name__ == "__main__":
    main()
