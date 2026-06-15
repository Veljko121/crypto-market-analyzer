from pyspark.sql import SparkSession
import sys

def main():
    coins_csv_path = sys.argv[1]
    historical_csv_path = sys.argv[2]
    transformed_data_folder = sys.argv[3]

    spark = SparkSession.builder.appName("transform_data").getOrCreate()

    df_coins = spark.read.csv(path=coins_csv_path, header=True, inferSchema=True)
    df_historical = spark.read.csv(path=historical_csv_path, header=True, inferSchema=True)

    historical_result = df_historical.select("date", "coin_id", "price", "circulating_supply", "volume_24h")
    coins_result = df_coins.select("id", "name")

    coins_result.write.csv(transformed_data_folder + "/coins.csv", mode="overwrite", header=True)
    historical_result.write.csv(transformed_data_folder + "/historical.csv", mode="overwrite", header=True)

if __name__ == "__main__":
    main()
