#!/bin/bash
set -e

docker compose exec spark spark-submit   --master local[*]   --packages org.postgresql:postgresql:42.7.4   /opt/project/spark_jobs/etl_to_postgres_star.py
