from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import sys

def main():
    coins_csv = sys.argv[1]
    historical_csv = sys.argv[2]
    transformed_data_folder = sys.argv[3]
    spark = SparkSession.builder.appName("transform_raw_data").getOrCreate()

    df_coins = spark.read.csv(
        path=coins_csv,
        header=True,
        inferSchema=True
    )

    df_historical = spark.read.csv(
        path=historical_csv,
        header=True,
        inferSchema=True
    )

    df_joined = df_historical.join(
        df_coins,
        df_historical.coin_id == df_coins.id,
        how="inner"
    )

    result = df_joined.select(
        df_coins.id.alias("id"),
        df_coins.name.alias("name"),
        to_date(df_historical.date).alias("date"),
        df_historical.price,
        df_historical.cmc_rank,
        df_historical.market_cap
    )

    result.write.parquet(transformed_data_folder + "/transformed.parquet", mode="overwrite")

if __name__ == "__main__":
    main()
