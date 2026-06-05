from airflow.sdk import dag, task, Param
from utils import spark_submit, HDFS_HOST, MONGO_HOST, transform_raw_csv_data

@dag(
    dag_id="market_movement",
    params={
        "transform_data": Param(False, type="boolean"),
        "start_date": Param("2013-04-28", type="string", format="date"),
        "end_date": Param("2021-07-31", type="string", format="date"),
    }
)
def market_movement():

    @task.bash
    def transform_data(**context) -> str:
        transform_data_boolean = bool(context["params"]["transform_data"])
        if not transform_data_boolean:
            return 'echo "Skipping transform"'
        return transform_raw_csv_data()

    @task.bash
    def calculate_market_movement(**context):
        params = context["params"]
        start_date = params["start_date"]
        end_date = params["end_date"]
        csv_data = HDFS_HOST + "/transformed_data/csv/historical.csv"
        mongo_host = MONGO_HOST
        mongo_db = "crypto"
        command = spark_submit("my_jobs/market_movement.py", packages=["org.mongodb.spark:mongo-spark-connector_2.13:10.6.0"], args=[csv_data, start_date, end_date, mongo_host, mongo_db])
        return command

    transform_data() >> calculate_market_movement()

market_movement()
