#!/bin/sh
set -e
docker run --env-file .env sentinel-airflow-spark driver local:///opt/application/export-parquet.py 
