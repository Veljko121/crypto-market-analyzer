from airflow.sdk import dag, task
from utils import spark_submit

@dag(
    dag_id="spark_connection_test",
    schedule=None,
    catchup=False,
)
def spark_connection_test():

    @task
    def test_spark_connection():
        spark_submit("my_jobs/test_connection.py")

    test_spark_connection()

spark_connection_test()
