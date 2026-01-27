from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    output_transformed_dir = sys.argv[1]

    spark = SparkSession.builder.appName("transform_dns_answers_data").getOrCreate()

    df = spark.read.parquet(f"{output_transformed_dir}/initial_data")

    spark.conf.set("spark.sql.caseSensitive", "true")

    dns_answers_df = (
        df.where(sf.col("payload.payload.payload.layer_type") == "DNS")
        .select(
            "packets_ts",
            "dt",
            sf.col("payload.payload.payload.id").alias("dns_id"),
            sf.explode_outer("payload.payload.payload.an").alias("answer"),
        )
        .select(
            "packets_ts",
            "dns_id",
            "dt",
            sf.col("answer.rrname").alias("rrname"),
            sf.col("answer.type").alias("rr_type"),
            sf.col("answer.rdata").alias("rdata"),
            sf.col("answer.ttl").alias("ttl"),
        )
    )

    dns_answers_df.write.mode("overwrite").partitionBy("dt").parquet(
        f"{output_transformed_dir}/dns_answers"
    )

    dns_answers_df.show()

    spark.conf.set("spark.sql.caseSensitive", "false")

    spark.stop()
