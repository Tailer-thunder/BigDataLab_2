from typing import List
import os
import urllib.request
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bigdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "bigdata")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bigdata")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_JDBC_PORT = os.getenv("CLICKHOUSE_JDBC_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "reports")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

POSTGRES_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    "jdbc:postgresql://{0}:{1}/{2}".format(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
)

CLICKHOUSE_URL = os.getenv(
    "CLICKHOUSE_JDBC_URL",
    "jdbc:ch://{0}:{1}/{2}".format(CLICKHOUSE_HOST, CLICKHOUSE_JDBC_PORT, CLICKHOUSE_DB)
)

POSTGRES_DRIVER = "org.postgresql.Driver"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def jdbc_read(spark: SparkSession, url: str, table_name: str, user: str, password: str, driver: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .load()
    )


def jdbc_write(df: DataFrame, url: str, table_name: str, user: str, password: str, driver: str, mode: str = "overwrite") -> None:
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .option("batchsize", "1000")
        .mode(mode)
        .save()
    )


def trim_and_nullify_strings(df: DataFrame, columns: List[str]) -> DataFrame:
    result = df
    for column_name in columns:
        result = result.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(column_name)))
        )
    return result


def add_business_key(df: DataFrame, column_name: str, columns: List[str]) -> DataFrame:
    safe_columns = [
        F.coalesce(F.col(current_column).cast("string"), F.lit("__NULL__"))
        for current_column in columns
    ]
    return df.withColumn(column_name, F.sha2(F.concat_ws("||", *safe_columns), 256))


def build_dimension(df: DataFrame, business_key: str, surrogate_key: str, attributes: List[str]) -> DataFrame:
    window_spec = Window.orderBy(F.col(business_key))
    return (
        df.select(business_key, *attributes)
        .dropDuplicates([business_key])
        .withColumn(surrogate_key, F.row_number().over(window_spec))
        .select(surrogate_key, *attributes, business_key)
    )


def execute_clickhouse_sql(sql_text: str) -> None:
    request = urllib.request.Request(
        url="http://{0}:{1}/".format(CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT),
        data=sql_text.encode("utf-8"),
        method="POST",
    )
    if CLICKHOUSE_USER:
        request.add_header("X-ClickHouse-User", CLICKHOUSE_USER)
    if CLICKHOUSE_PASSWORD:
        request.add_header("X-ClickHouse-Key", CLICKHOUSE_PASSWORD)

    with urllib.request.urlopen(request) as response:
        response.read()