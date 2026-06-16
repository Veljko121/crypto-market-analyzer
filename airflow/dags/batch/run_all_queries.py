from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="run_all_batch",
    schedule=None,
    catchup=False,
    tags=["batch"],
)
def run_all_batch():

    trigger_average_coin_price = TriggerDagRunOperator(
        task_id="trigger_average_coin_price",
        trigger_dag_id="average_coin_price",
        wait_for_completion=False,
    )

    trigger_top_coins_trends = TriggerDagRunOperator(
        task_id="trigger_top_coins_trends",
        trigger_dag_id="top_coins_trends",
        wait_for_completion=False,
    )

    trigger_market_cap_distribution = TriggerDagRunOperator(
        task_id="trigger_market_cap_distribution",
        trigger_dag_id="market_cap_distribution",
        wait_for_completion=False,
    )

    trigger_coin_race = TriggerDagRunOperator(
        task_id="trigger_coin_race",
        trigger_dag_id="coin_race",
        wait_for_completion=False,
    )

    trigger_monthly_volume = TriggerDagRunOperator(
        task_id="trigger_monthly_volume",
        trigger_dag_id="monthly_volume_for_year",
        wait_for_completion=False,
    )

    # Pokreni sve paralelno
    [
        trigger_average_coin_price,
        trigger_top_coins_trends,
        trigger_market_cap_distribution,
        trigger_coin_race,
        trigger_monthly_volume,
    ]

run_all_batch()