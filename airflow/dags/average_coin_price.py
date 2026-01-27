from airflow.sdk import dag, task
from utils import spark_submit, HDFS_HOST, MONGO_HOST

@dag(
    dag_id="average_coin_price",
    schedule=None,
    catchup=False,
)
def bash_spark_test():

    @task.bash
    def calculate_average_price():
        transformed_csv = HDFS_HOST + "/transformed_data/transformed.csv"
        mongo_host = MONGO_HOST
        mongo_db = "crypto"
        command = spark_submit("my_jobs/average_price.py", packages=["org.mongodb.spark:mongo-spark-connector_2.13:10.6.0"], args=[transformed_csv, mongo_host, mongo_db])
        return command
    
    calculate_average_price()

bash_spark_test()
