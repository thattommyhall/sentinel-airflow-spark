#!/bin/sh
set -e
docker build -t sentinel-airflow-spark .
docker tag sentinel-airflow-spark thattommyhall/sentinel-airflow-spark:latest
docker push thattommyhall/sentinel-airflow-spark:latest 