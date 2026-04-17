# Запуск проекта


## 1. Поднять контейнеры в корне проекта
```powershell
docker compose up -d
```
Запускает PostgreSQL, ClickHouse и Spark в Docker.

## 2. Построить звезду в PostgreSQL
```powershell
docker compose exec spark /bin/bash -lc "mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit --master local[*] --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.4 /opt/project/spark_jobs/etl_to_postgres_star.py"
```
Spark читает исходные CSV, загружает сырые данные и строит схему «звезда» в PostgreSQL.

## 3. Построить 6 витрин в ClickHouse
```powershell
docker compose exec spark /bin/bash -lc "mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit --master local[*] --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.6.3 /opt/project/spark_jobs/clickhouse_reports.py"
```
Spark читает данные из PostgreSQL и создаёт 6 итоговых витрин в ClickHouse.

# Проверка результата

## 4. Открыть ClickHouse
```powershell
docker exec -it bigdata-clickhouse clickhouse-client
```
открывает консоль ClickHouse для проверки созданных витрин.

## 5. Проверить витрины
```sql
SHOW TABLES FROM reports;
SELECT * FROM reports.report_product_sales LIMIT 10;
SELECT * FROM reports.report_customer_sales LIMIT 10;
SELECT * FROM reports.report_time_sales LIMIT 10;
SELECT * FROM reports.report_store_sales LIMIT 5;
SELECT * FROM reports.report_supplier_sales LIMIT 5;
SELECT * FROM reports.report_product_quality LIMIT 10;
```
Показывает список таблиц и содержимое 6 необходимых витрин.
