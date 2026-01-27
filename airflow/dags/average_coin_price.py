from airflow.sdk import dag, task
from utils import spark_submit

@dag(
    dag_id="average_coin_price",
    schedule=None,
    catchup=False,
)
def bash_spark_test():

    @task.bash
    def method():
        return spark_submit("my_jobs/avg_price.py")
    
    method()

bash_spark_test()
