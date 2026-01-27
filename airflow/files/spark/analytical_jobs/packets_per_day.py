from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    packets_dir = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("packets_per_day").getOrCreate()

    packets = spark.read.parquet(packets_dir)

    df = packets.groupBy("dt").count().withColumnRenamed("count", "packet_count")

    df.show()

    df.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "packets_per_day").save()

    spark.stop()
