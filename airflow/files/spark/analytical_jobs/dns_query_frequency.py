from pyspark.sql import SparkSession, functions as sf, Window
import sys

if __name__ == "__main__":
    dns_queries_dir = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("dns_queries_frequency").getOrCreate()

    dns_queries = spark.read.parquet(dns_queries_dir)

    w = Window.partitionBy("qname").orderBy("packets_ts")

    df = dns_queries.withColumn("query_sequence_number", sf.row_number().over(w))

    df.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "dns_queries_frequency").save()

    spark.stop()
