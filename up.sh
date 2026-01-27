#!/bin/bash

echo ">> Creating crypto-analyzer network"
docker network create crypto-analyzer

echo ">> Starting up HDFS"
docker compose -f hdfs/docker-compose.yml up -d

echo ">> Starting up Spark"
docker compose -f spark/docker-compose.yml up -d

echo ">> Starting up MongoDB"
docker compose -f mongodb/docker-compose.yml up -d

echo ">> Starting up Airflow"
docker compose -f airflow/docker-compose.yml up -d

echo Cluster is up!
