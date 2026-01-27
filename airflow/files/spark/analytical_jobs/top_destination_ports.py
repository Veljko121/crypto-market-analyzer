from pyspark.sql import SparkSession, functions as sf, Window
import sys

if __name__ == "__main__":
    transport_flows_dir = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("top_destination_ports").getOrCreate()

    transport = spark.read.parquet(transport_flows_dir)

    port_counts = transport.groupBy("dt", "dst_port").count()

    w = Window.partitionBy("dt").orderBy(sf.desc("count"))

    df = port_counts.withColumn("port_rank", sf.row_number().over(w)).where(
        sf.col("port_rank") <= 5
    )

    df.write.format("mongodb").mode("overwrite").option(
        "connection.uri", mongo_uri
    ).option("database", mongo_db).option("collection", "top_destination_ports").save()

    spark.stop()
