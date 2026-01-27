from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_transformed_dir = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_initial_data").getOrCreate()

    df = (
        spark.read.json(input_file_path)
        .withColumn(
            "packets_ts",
            sf.to_timestamp("packet_received_at", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("dt", sf.to_date("packets_ts"))
    )

    df.write.mode("overwrite").partitionBy("dt").parquet(
        f"{output_transformed_dir}/initial_data"
    )
    
    df.show()

    spark.stop()