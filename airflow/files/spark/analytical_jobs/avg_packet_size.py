from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    ip_flows_dir = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("avg_packet_size").getOrCreate()

    ip_flows = spark.read.parquet(ip_flows_dir)

    df = ip_flows.groupBy("dt").agg(sf.avg("ip_len").alias("avg_packet_size"))

    df.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "avg_packet_size").save()

    spark.stop()
