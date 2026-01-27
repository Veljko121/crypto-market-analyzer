#!/bin/bash

echo ">> Shutting down Metabase"
docker compose -f metabase/docker-compose.yml down

echo ">> Shutting down Airflow"
docker compose -f airflow/docker-compose.yml down

echo ">> Shutting down MongoDB"
docker compose -f mongodb/docker-compose.yml down

echo ">> Shutting down Apache Spark"
docker compose -f spark/docker-compose.yml down

echo ">> Shutting down HDFS"
docker compose -f hdfs/docker-compose.yml down

echo ">> Removing crypto-analyzer network"
docker network rm crypto-analyzer

echo Cluster is down.
