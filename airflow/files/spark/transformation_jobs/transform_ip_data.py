from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    output_transformed_dir = sys.argv[1]

    spark = SparkSession.builder.appName("transform_ip_data").getOrCreate()

    df = spark.read.parquet(f"{output_transformed_dir}/initial_data")

    ip_df = df.select(
        sf.col("payload.src").alias("ip_src"),
        sf.col("payload.dst").alias("ip_dst"),
        sf.col("payload.proto").alias("ip_proto"),
        sf.col("payload.ttl").alias("ip_ttl"),
        sf.col("payload.len").alias("ip_len"),
        sf.col("payload.version").alias("ip_version"),
        "packets_ts",
        "dt",
    ).where(sf.col("payload.layer_type") == "IP")

    ip_df.write.mode("overwrite").partitionBy("dt").parquet(
        f"{output_transformed_dir}/ip_flows"
    )

    ip_df.show()

    spark.stop()
