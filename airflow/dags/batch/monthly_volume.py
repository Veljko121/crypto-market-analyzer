from airflow.sdk import dag, task, Param
from utils import spark_submit, HDFS_HOST, MONGO_HOST, transform_raw_csv_data

@dag(
    dag_id="monthly_volume_for_year",
    schedule=None,
    catchup=False,
    params={
        "transform_data": Param(False, type="boolean"),
        "year": Param(2013, type="number"),
    },
    tags=["batch"],
)
def monthly_volume_for_year():

    @task.bash
    def transform_data(**context) -> str:
        transform_data_boolean = bool(context["params"]["transform_data"])
        if not transform_data_boolean:
            return 'echo "Skipping transform"'
        return transform_raw_csv_data()
    
    @task.bash
    def calculate_monthly_volume_for_year(**context):
        params = context["params"]
        year = params["year"]

        historical_data = HDFS_HOST + "/transformed_data/csv/historical.csv"
        coins_data = HDFS_HOST + "/transformed_data/csv/coins.csv"
        mongo_host = MONGO_HOST
        mongo_db = "crypto"

        command = spark_submit("my_jobs/monthly_volume_for_year.py", packages=["org.mongodb.spark:mongo-spark-connector_2.13:10.6.0"], args=[historical_data, coins_data, year, mongo_host, mongo_db])
        return command
    
    transform_data() >> calculate_monthly_volume_for_year()

monthly_volume_for_year()
