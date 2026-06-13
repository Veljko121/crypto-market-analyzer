from airflow.decorators import dag, task
from datetime import datetime
import requests
import json
from kafka import KafkaProducer
from datetime import datetime, timezone

@dag(
    dag_id="fetch_crypto_prices",
    schedule="* * * * *",  # svakog minuta
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["streaming"],
)
def fetch_crypto_prices():

    @task
    def fetch_and_send():
        API_KEY = "CG-6wzjCQKe8n3NAamgE6ins44S"
        KAFKA_HOST = "broker-1:9092,broker-2:9092"  # interni Docker host
        TOPIC = "crypto_prices_live"

        url = "https://pro-api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "x_cg_pro_api_key": API_KEY
        }

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_HOST,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        response = requests.get(url, params=params)
        response.raise_for_status()
        coins = response.json()

        for coin in coins:
            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "id": coin["id"],
                "name": coin["name"],
                "current_price": coin["current_price"],
                "circulating_supply": coin["circulating_supply"],
                "total_volume": coin["total_volume"],
            }
            producer.send(TOPIC, key=coin["id"].encode("utf-8"), value=message)

        producer.flush()
        print(f"Sent {len(coins)} coins to Kafka topic '{TOPIC}'")

    fetch_and_send()

fetch_crypto_prices()
