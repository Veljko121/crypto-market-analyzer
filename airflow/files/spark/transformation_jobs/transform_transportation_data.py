from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    output_transformed_dir = sys.argv[1]

    spark = SparkSession.builder.appName("transform_transportation_data").getOrCreate()

    df = spark.read.parquet(f"{output_transformed_dir}/initial_data")

    transport_df = df.select(
        sf.col("payload.payload.layer_type").alias("l4_protocol"),
        sf.col("payload.payload.src").alias("l4_src_ip"),
        sf.col("payload.payload.dst").alias("l4_dst_ip"),
        sf.col("payload.payload.sport").alias("src_port"),
        sf.col("payload.payload.dport").alias("dst_port"),
        sf.col("payload.payload.len").alias("l4_len"),
        "packets_ts",
        "dt",
    ).where(sf.col("payload.payload.layer_type").isin("TCP", "UDP"))

    transport_df.write.mode("overwrite").partitionBy("dt").parquet(
        f"{output_transformed_dir}/transport_flows"
    )

    transport_df.show()

    spark.stop()
