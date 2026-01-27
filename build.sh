#!/bin/bash

echo ">> Pulling HDFS images"
docker compose -f hdfs/docker-compose.yml pull

echo ">> Pulling Spark images"
docker compose -f spark/docker-compose.yml pull

echo ">> Pulling MongoDB images"
docker compose -f mongodb/docker-compose.yml pull

echo ">> Pulling and building Airflow images"
docker compose -f airflow/docker-compose.yml pull
docker compose -f airflow/docker-compose.yml build

echo All images pulled and built!
