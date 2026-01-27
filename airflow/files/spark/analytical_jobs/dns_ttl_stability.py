from pyspark.sql import SparkSession, functions as sf, Window
import sys

if __name__ == "__main__":
    dns_answers_dir = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("dns_ttl_stability").getOrCreate()

    dns_answers = spark.read.parquet(dns_answers_dir)

    w = Window.partitionBy("rrname").orderBy("packets_ts")

    df = dns_answers.withColumn("prev_ttl", sf.lag("ttl").over(w)).withColumn(
        "ttl_delta", sf.col("ttl") - sf.col("prev_ttl")
    )

    df.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "dns_ttl_stability").save()

    spark.stop()
