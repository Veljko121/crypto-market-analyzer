from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    output_transformed_dir = sys.argv[1]

    spark = SparkSession.builder.appName("transform_dns_queries_data").getOrCreate()

    df = spark.read.parquet(f"{output_transformed_dir}/initial_data")

    spark.conf.set("spark.sql.caseSensitive", "true")

    dns_queries_df = (
        df.where(sf.col("payload.payload.payload.layer_type") == "DNS")
        .select(
            "packets_ts",
            "dt",
            sf.col("payload.payload.payload.id").alias("dns_id"),
            sf.explode_outer("payload.payload.payload.qd").alias("question"),
        )
        .select(
            "packets_ts",
            "dt",
            "dns_id",
            sf.col("question.qname").alias("qname"),
            sf.col("question.qtype").alias("qtype"),
            sf.col("question.qclass").alias("qclass"),
        )
    )

    dns_queries_df.write.mode("overwrite").partitionBy("dt").parquet(
        f"{output_transformed_dir}/dns_queries"
    )

    dns_queries_df.show()

    spark.conf.set("spark.sql.caseSensitive", "false")

    spark.stop()
