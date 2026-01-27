from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    output_transformed_dir = sys.argv[1]

    spark = SparkSession.builder.appName("transform_packets_data").getOrCreate()

    df = spark.read.parquet(f"{output_transformed_dir}/initial_data")

    packets_df = df.select(
        sf.monotonically_increasing_id().alias("packet_id"),
        "dt",
        "packets_ts",
        "src",
        "dst",
        "type",
        "layer_type",
    )

    packets_df.write.mode("overwrite").partitionBy("dt").parquet(
        f"{output_transformed_dir}/packets"
    )

    packets_df.show()

    spark.stop()