#!/bin/bash
set -e

docker compose exec spark spark-submit   --master local[*]   --packages org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.6.3:all   /opt/project/spark_jobs/clickhouse_reports.py
