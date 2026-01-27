from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

def main():
    spark = SparkSession.builder.appName("avg_coin_price").getOrCreate()

    print("📂 Reading data from HDFS...")
    df = spark.read.csv(
        "webhdfs://namenode:9870/raw_data/historical.csv",
        header=True,
        inferSchema=True
    )

    print(f"✅ Loaded {df.count()} rows")

    avg_price = df.select(mean("price")).collect()[0][0]
    print(f"📊 Average coin price: ${avg_price:.2f}")

    spark.stop()
    
if __name__ == "__main__":
    main()
