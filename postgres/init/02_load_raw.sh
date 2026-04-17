#!/bin/sh
set -e

echo "PostgreSQL init: skip raw CSV loading here."
echo "Raw data will be loaded by Spark ETL."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT 1;"