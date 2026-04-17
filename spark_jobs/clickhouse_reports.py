\from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    CLICKHOUSE_DB,
    CLICKHOUSE_DRIVER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
    POSTGRES_DRIVER,
    POSTGRES_PASSWORD,
    POSTGRES_URL,
    POSTGRES_USER,
    create_spark_session,
    execute_clickhouse_sql,
    jdbc_read,
    jdbc_write,
)


def write_clickhouse_table(df, table_name: str, create_sql: str) -> None:
    execute_clickhouse_sql(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB};")
    execute_clickhouse_sql(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{table_name};")
    execute_clickhouse_sql(create_sql)

    jdbc_write(
        df.coalesce(1),
        CLICKHOUSE_URL,
        f"{CLICKHOUSE_DB}.{table_name}",
        CLICKHOUSE_USER,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_DRIVER,
        mode="append",
    )


def drop_old_views() -> None:
    old_views = [
        "vw_product_top10",
        "vw_product_category_revenue",
        "vw_product_rating_reviews",
        "vw_customer_top10",
        "vw_customer_country_distribution",
        "vw_customer_avg_check",
        "vw_time_monthly_yearly_trends",
        "vw_time_revenue_comparison",
        "vw_time_avg_order_size",
        "vw_store_top5",
        "vw_store_city_country_distribution",
        "vw_store_avg_check",
        "vw_supplier_top5",
        "vw_supplier_avg_product_price",
        "vw_supplier_country_distribution",
        "vw_quality_highest_rated_products",
        "vw_quality_lowest_rated_products",
        "vw_quality_rating_sales_correlation",
        "vw_quality_most_reviewed_products",
    ]

    for view_name in old_views:
        execute_clickhouse_sql(f"DROP VIEW IF EXISTS {CLICKHOUSE_DB}.{view_name};")


def main() -> None:
    spark = create_spark_session("postgres_star_to_clickhouse_reports")

    execute_clickhouse_sql(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB};")
    drop_old_views()

    fact_sales = jdbc_read(
        spark, POSTGRES_URL, "dwh.fact_sales", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER
    )
    dim_date = jdbc_read(
        spark, POSTGRES_URL, "dwh.dim_date", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER
    )
    dim_customer = jdbc_read(
        spark, POSTGRES_URL, "dwh.dim_customer", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER
    )
    dim_product = jdbc_read(
        spark, POSTGRES_URL, "dwh.dim_product", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER
    )
    dim_store = jdbc_read(
        spark, POSTGRES_URL, "dwh.dim_store", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER
    )
    dim_supplier = jdbc_read(
        spark, POSTGRES_URL, "dwh.dim_supplier", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER
    )

    sales = (
        fact_sales
        .join(dim_date, on="date_key", how="left")
        .join(dim_customer, on="customer_key", how="left")
        .join(dim_product, on="product_key", how="left")
        .join(dim_store, on="store_key", how="left")
        .join(dim_supplier, on="supplier_key", how="left")
    )

    # ============================================================
    # 1. ВИТРИНА ПРОДАЖ ПО ПРОДУКТАМ
    # ============================================================

    product_base = (
        sales.groupBy("product_key", "product_name", "product_category")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.sum("sale_quantity").alias("total_units_sold"),
            F.count("fact_sale_key").alias("order_count"),
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.sum("product_reviews").alias("total_reviews"),
        )
    )

    product_rank_window = Window.orderBy(
        F.desc("total_units_sold"),
        F.desc("total_revenue"),
        F.asc("product_key"),
    )
    product_category_window = Window.partitionBy("product_category")

    report_product_sales = (
        product_base
        .withColumn("sales_rank", F.row_number().over(product_rank_window))
        .withColumn(
            "category_revenue",
            F.round(F.sum("total_revenue").over(product_category_window), 2),
        )
        .select(
            "product_key",
            "product_name",
            "product_category",
            "total_units_sold",
            "total_revenue",
            "order_count",
            "sales_rank",
            "category_revenue",
            "avg_rating",
            "total_reviews",
        )
    )

    # ============================================================
    # 2. ВИТРИНА ПРОДАЖ ПО КЛИЕНТАМ
    # ============================================================

    customer_base = (
        sales.groupBy(
            "customer_key",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
            F.count("fact_sale_key").alias("order_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .withColumn(
            "customer_full_name",
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce("customer_first_name", F.lit("")),
                    F.coalesce("customer_last_name", F.lit("")),
                )
            ),
        )
    )

    customer_rank_window = Window.orderBy(F.desc("total_spent"), F.asc("customer_key"))
    customer_country_window = Window.partitionBy("customer_country")

    report_customer_sales = (
        customer_base
        .withColumn("spending_rank", F.row_number().over(customer_rank_window))
        .withColumn(
            "country_customer_count",
            F.count("customer_key").over(customer_country_window),
        )
        .withColumn(
            "country_total_spent",
            F.round(F.sum("total_spent").over(customer_country_window), 2),
        )
        .select(
            "customer_key",
            "customer_full_name",
            "customer_email",
            "customer_country",
            "total_spent",
            "order_count",
            "avg_check",
            "spending_rank",
            "country_customer_count",
            "country_total_spent",
        )
    )

    # ============================================================
    # 3. ВИТРИНА ПРОДАЖ ПО ВРЕМЕНИ
    # ============================================================

    time_base = (
        sales
        .withColumn("month_start", F.trunc("sale_date", "month"))
        .groupBy("year_number", "month_number", "month_start")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("monthly_revenue"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_size"),
            F.count("fact_sale_key").alias("order_count"),
        )
    )

    time_window = Window.orderBy("month_start")
    same_month_last_year_window = Window.partitionBy("month_number").orderBy("year_number")

    report_time_sales = (
        time_base
        .withColumn(
            "yearly_revenue",
            F.round(F.sum("monthly_revenue").over(Window.partitionBy("year_number")), 2),
        )
        .withColumn("previous_month_revenue", F.lag("monthly_revenue").over(time_window))
        .withColumn(
            "previous_year_same_month_revenue",
            F.lag("monthly_revenue").over(same_month_last_year_window),
        )
        .orderBy("month_start")
        .select(
            "year_number",
            "month_number",
            "month_start",
            "monthly_revenue",
            "yearly_revenue",
            "previous_month_revenue",
            "previous_year_same_month_revenue",
            "avg_order_size",
            "order_count",
        )
    )

    # ============================================================
    # 4. ВИТРИНА ПРОДАЖ ПО МАГАЗИНАМ
    # ============================================================

    store_base = (
        sales.groupBy("store_key", "store_name", "store_city", "store_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.count("fact_sale_key").alias("order_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
    )

    store_rank_window = Window.orderBy(F.desc("total_revenue"), F.asc("store_key"))
    store_city_window = Window.partitionBy("store_city", "store_country")
    store_country_window = Window.partitionBy("store_country")

    report_store_sales = (
        store_base
        .withColumn("revenue_rank", F.row_number().over(store_rank_window))
        .withColumn(
            "city_revenue",
            F.round(F.sum("total_revenue").over(store_city_window), 2),
        )
        .withColumn(
            "country_revenue",
            F.round(F.sum("total_revenue").over(store_country_window), 2),
        )
        .select(
            "store_key",
            "store_name",
            "store_city",
            "store_country",
            "total_revenue",
            "order_count",
            "avg_check",
            "revenue_rank",
            "city_revenue",
            "country_revenue",
        )
    )

    # ============================================================
    # 5. ВИТРИНА ПРОДАЖ ПО ПОСТАВЩИКАМ
    # ============================================================

    supplier_base = (
        sales.groupBy("supplier_key", "supplier_name", "supplier_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.sum("sale_quantity").alias("total_units_sold"),
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
            F.count("fact_sale_key").alias("order_count"),
        )
    )

    supplier_rank_window = Window.orderBy(F.desc("total_revenue"), F.asc("supplier_key"))
    supplier_country_window = Window.partitionBy("supplier_country")

    report_supplier_sales = (
        supplier_base
        .withColumn("revenue_rank", F.row_number().over(supplier_rank_window))
        .withColumn(
            "supplier_country_revenue",
            F.round(F.sum("total_revenue").over(supplier_country_window), 2),
        )
        .select(
            "supplier_key",
            "supplier_name",
            "supplier_country",
            "total_revenue",
            "total_units_sold",
            "avg_product_price",
            "order_count",
            "revenue_rank",
            "supplier_country_revenue",
        )
    )

    # ============================================================
    # 6. ВИТРИНА КАЧЕСТВА ПРОДУКЦИИ
    # ============================================================

    quality_base = (
        sales.groupBy("product_key", "product_name", "product_category")
        .agg(
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.sum("product_reviews").alias("review_count"),
            F.sum("sale_quantity").alias("total_units_sold"),
        )
    )

    correlation_row = quality_base.select(
        F.corr("avg_rating", "total_units_sold").alias("corr_value")
    ).collect()[0]
    correlation_value = correlation_row["corr_value"]
    if correlation_value is None:
        correlation_value = 0.0

    highest_rating_window = Window.orderBy(
        F.desc("avg_rating"),
        F.desc("review_count"),
        F.asc("product_key"),
    )
    lowest_rating_window = Window.orderBy(
        F.asc("avg_rating"),
        F.asc("review_count"),
        F.asc("product_key"),
    )

    report_product_quality = (
        quality_base
        .withColumn("highest_rating_rank", F.row_number().over(highest_rating_window))
        .withColumn("lowest_rating_rank", F.row_number().over(lowest_rating_window))
        .withColumn("rating_sales_correlation", F.lit(float(correlation_value)))
        .select(
            "product_key",
            "product_name",
            "product_category",
            "avg_rating",
            "review_count",
            "total_units_sold",
            "highest_rating_rank",
            "lowest_rating_rank",
            "rating_sales_correlation",
        )
    )

    write_clickhouse_table(
        report_product_sales,
        "report_product_sales",
        f"""
        CREATE TABLE {CLICKHOUSE_DB}.report_product_sales
        (
            product_key UInt64,
            product_name String,
            product_category Nullable(String),
            total_units_sold Int64,
            total_revenue Float64,
            order_count Int64,
            sales_rank Int64,
            category_revenue Float64,
            avg_rating Float64,
            total_reviews Int64
        )
        ENGINE = MergeTree
        ORDER BY (sales_rank, product_key)
        """.strip(),
    )

    write_clickhouse_table(
        report_customer_sales,
        "report_customer_sales",
        f"""
        CREATE TABLE {CLICKHOUSE_DB}.report_customer_sales
        (
            customer_key UInt64,
            customer_full_name String,
            customer_email Nullable(String),
            customer_country Nullable(String),
            total_spent Float64,
            order_count Int64,
            avg_check Float64,
            spending_rank Int64,
            country_customer_count Int64,
            country_total_spent Float64
        )
        ENGINE = MergeTree
        ORDER BY (spending_rank, customer_key)
        """.strip(),
    )

    write_clickhouse_table(
        report_time_sales,
        "report_time_sales",
        f"""
        CREATE TABLE {CLICKHOUSE_DB}.report_time_sales
        (
            year_number Int64,
            month_number Int64,
            month_start Date,
            monthly_revenue Float64,
            yearly_revenue Float64,
            previous_month_revenue Nullable(Float64),
            previous_year_same_month_revenue Nullable(Float64),
            avg_order_size Float64,
            order_count Int64
        )
        ENGINE = MergeTree
        ORDER BY (month_start)
        """.strip(),
    )

    write_clickhouse_table(
        report_store_sales,
        "report_store_sales",
        f"""
        CREATE TABLE {CLICKHOUSE_DB}.report_store_sales
        (
            store_key UInt64,
            store_name Nullable(String),
            store_city Nullable(String),
            store_country Nullable(String),
            total_revenue Float64,
            order_count Int64,
            avg_check Float64,
            revenue_rank Int64,
            city_revenue Float64,
            country_revenue Float64
        )
        ENGINE = MergeTree
        ORDER BY (revenue_rank, store_key)
        """.strip(),
    )

    write_clickhouse_table(
        report_supplier_sales,
        "report_supplier_sales",
        f"""
        CREATE TABLE {CLICKHOUSE_DB}.report_supplier_sales
        (
            supplier_key UInt64,
            supplier_name Nullable(String),
            supplier_country Nullable(String),
            total_revenue Float64,
            total_units_sold Int64,
            avg_product_price Float64,
            order_count Int64,
            revenue_rank Int64,
            supplier_country_revenue Float64
        )
        ENGINE = MergeTree
        ORDER BY (revenue_rank, supplier_key)
        """.strip(),
    )

    write_clickhouse_table(
        report_product_quality,
        "report_product_quality",
        f"""
        CREATE TABLE {CLICKHOUSE_DB}.report_product_quality
        (
            product_key UInt64,
            product_name String,
            product_category Nullable(String),
            avg_rating Float64,
            review_count Int64,
            total_units_sold Int64,
            highest_rating_rank Int64,
            lowest_rating_rank Int64,
            rating_sales_correlation Float64
        )
        ENGINE = MergeTree
        ORDER BY (highest_rating_rank, product_key)
        """.strip(),
    )

    print("Six ClickHouse reports were created successfully.")
    print(f"report_product_sales rows: {report_product_sales.count()}")
    print(f"report_customer_sales rows: {report_customer_sales.count()}")
    print(f"report_time_sales rows: {report_time_sales.count()}")
    print(f"report_store_sales rows: {report_store_sales.count()}")
    print(f"report_supplier_sales rows: {report_supplier_sales.count()}")
    print(f"report_product_quality rows: {report_product_quality.count()}")

    spark.stop()


if __name__ == "__main__":
    main()