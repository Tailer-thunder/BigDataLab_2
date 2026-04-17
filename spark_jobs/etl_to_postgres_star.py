import os
from typing import List
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (
    POSTGRES_DRIVER,
    POSTGRES_PASSWORD,
    POSTGRES_URL,
    POSTGRES_USER,
    add_business_key,
    build_dimension,
    create_spark_session,
    jdbc_write,
    trim_and_nullify_strings,
)


RAW_COLUMNS = [
    "source_row_id",
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
    "product_name",
    "product_category",
    "product_price",
    "product_quantity",
    "sale_date",
    "sale_customer_id",
    "sale_seller_id",
    "sale_product_id",
    "sale_quantity",
    "sale_total_price",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
    "pet_category",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]

CUSTOMER_COLUMNS = [
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
]

SELLER_COLUMNS = [
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
]

PRODUCT_COLUMNS = [
    "product_name",
    "product_category",
    "product_price",
    "product_quantity",
    "pet_category",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
]

STORE_COLUMNS = [
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
]

SUPPLIER_COLUMNS = [
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]


def discover_csv_files(root_path: str):
    csv_files = []

    for current_root, _, files in os.walk(root_path):
        for file_name in files:
            if file_name.lower().endswith(".csv"):
                csv_files.append(os.path.join(current_root, file_name))

    csv_files.sort()
    return csv_files


def main() -> None:
    spark = create_spark_session("etl_to_postgres_star_schema")

    csv_files = discover_csv_files("/opt/project")

    if len(csv_files) == 0:
        raise RuntimeError("CSV files not found under /opt/project")

    print("CSV files found:")
    for file_name in csv_files:
        print(file_name)

    raw_df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(csv_files)
    )

    if "id" in raw_df.columns:
        raw_df = raw_df.withColumnRenamed("id", "source_row_id")

    for column_name in RAW_COLUMNS:
        if column_name not in raw_df.columns:
            raw_df = raw_df.withColumn(column_name, F.lit(None).cast("string"))

    raw_df = raw_df.select(*RAW_COLUMNS)

    string_columns = [column_name for column_name, data_type in raw_df.dtypes if data_type == "string"]
    raw_df = trim_and_nullify_strings(raw_df, string_columns)

    raw_df = raw_df.withColumn("_row_order", F.monotonically_increasing_id())

    raw_window = Window.orderBy(F.col("_row_order"))

    raw_df = (
        raw_df
        .withColumn("raw_sale_key", F.row_number().over(raw_window))
        .drop("_row_order")
        .select("raw_sale_key", *RAW_COLUMNS)
    )

    jdbc_write(
        raw_df,
        POSTGRES_URL,
        "raw.mock_data",
        POSTGRES_USER,
        POSTGRES_PASSWORD,
        POSTGRES_DRIVER,
        "overwrite"
    )

    cleaned_df = (
        raw_df
        .withColumn("source_row_id", F.col("source_row_id").cast("int"))
        .withColumn("customer_age", F.col("customer_age").cast("int"))
        .withColumn("product_price", F.col("product_price").cast("double"))
        .withColumn("product_quantity", F.col("product_quantity").cast("int"))
        .withColumn("sale_quantity", F.col("sale_quantity").cast("int"))
        .withColumn("sale_total_price", F.col("sale_total_price").cast("double"))
        .withColumn("product_weight", F.col("product_weight").cast("double"))
        .withColumn("product_rating", F.col("product_rating").cast("double"))
        .withColumn("product_reviews", F.col("product_reviews").cast("int"))
        .withColumn("sale_customer_id", F.col("sale_customer_id").cast("int"))
        .withColumn("sale_seller_id", F.col("sale_seller_id").cast("int"))
        .withColumn("sale_product_id", F.col("sale_product_id").cast("int"))
        .withColumn("sale_date", F.to_date("sale_date", "M/d/yyyy"))
        .withColumn("product_release_date", F.to_date("product_release_date", "M/d/yyyy"))
        .withColumn("product_expiry_date", F.to_date("product_expiry_date", "M/d/yyyy"))
    )

    cleaned_df = add_business_key(cleaned_df, "customer_bk", CUSTOMER_COLUMNS)
    cleaned_df = add_business_key(cleaned_df, "seller_bk", SELLER_COLUMNS)
    cleaned_df = add_business_key(cleaned_df, "product_bk", PRODUCT_COLUMNS)
    cleaned_df = add_business_key(cleaned_df, "store_bk", STORE_COLUMNS)
    cleaned_df = add_business_key(cleaned_df, "supplier_bk", SUPPLIER_COLUMNS)

    dim_customer = build_dimension(cleaned_df, "customer_bk", "customer_key", CUSTOMER_COLUMNS)
    dim_seller = build_dimension(cleaned_df, "seller_bk", "seller_key", SELLER_COLUMNS)
    dim_product = build_dimension(cleaned_df, "product_bk", "product_key", PRODUCT_COLUMNS)
    dim_store = build_dimension(cleaned_df, "store_bk", "store_key", STORE_COLUMNS)
    dim_supplier = build_dimension(cleaned_df, "supplier_bk", "supplier_key", SUPPLIER_COLUMNS)

    dim_date = (
        cleaned_df.select("sale_date")
        .where(F.col("sale_date").isNotNull())
        .dropDuplicates(["sale_date"])
        .withColumn("date_key", F.date_format("sale_date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_month", F.dayofmonth("sale_date"))
        .withColumn("month_number", F.month("sale_date"))
        .withColumn("month_name", F.date_format("sale_date", "MMMM"))
        .withColumn("quarter_number", F.quarter("sale_date"))
        .withColumn("year_number", F.year("sale_date"))
        .withColumn("week_of_year", F.weekofyear("sale_date"))
        .select(
            "date_key",
            "sale_date",
            "day_of_month",
            "month_number",
            "month_name",
            "quarter_number",
            "year_number",
            "week_of_year",
        )
    )

    fact_window = Window.orderBy(F.col("raw_sale_key"))

    fact_sales = (
        cleaned_df
        .join(dim_customer.select("customer_key", "customer_bk"), on="customer_bk", how="left")
        .join(dim_seller.select("seller_key", "seller_bk"), on="seller_bk", how="left")
        .join(dim_product.select("product_key", "product_bk"), on="product_bk", how="left")
        .join(dim_store.select("store_key", "store_bk"), on="store_bk", how="left")
        .join(dim_supplier.select("supplier_key", "supplier_bk"), on="supplier_bk", how="left")
        .join(
            dim_date.select(F.col("sale_date").alias("dim_sale_date"), "date_key"),
            cleaned_df.sale_date == F.col("dim_sale_date"),
            how="left"
        )
        .withColumn("fact_sale_key", F.row_number().over(fact_window))
        .withColumn("loaded_at", F.current_timestamp())
        .select(
            "fact_sale_key",
            "raw_sale_key",
            "source_row_id",
            "sale_customer_id",
            "sale_seller_id",
            "sale_product_id",
            "customer_key",
            "seller_key",
            "product_key",
            "store_key",
            "supplier_key",
            "date_key",
            "sale_quantity",
            "sale_total_price",
            "loaded_at",
        )
    )

    jdbc_write(dim_date, POSTGRES_URL, "dwh.dim_date", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "overwrite")
    jdbc_write(dim_customer, POSTGRES_URL, "dwh.dim_customer", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "overwrite")
    jdbc_write(dim_seller, POSTGRES_URL, "dwh.dim_seller", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "overwrite")
    jdbc_write(dim_product, POSTGRES_URL, "dwh.dim_product", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "overwrite")
    jdbc_write(dim_store, POSTGRES_URL, "dwh.dim_store", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "overwrite")
    jdbc_write(dim_supplier, POSTGRES_URL, "dwh.dim_supplier", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "overwrite")
    jdbc_write(fact_sales, POSTGRES_URL, "dwh.fact_sales", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "overwrite")

    print("ETL to PostgreSQL star schema finished successfully.")
    print(f"raw.mock_data rows: {raw_df.count()}")
    print(f"dim_date rows: {dim_date.count()}")
    print(f"dim_customer rows: {dim_customer.count()}")
    print(f"dim_seller rows: {dim_seller.count()}")
    print(f"dim_product rows: {dim_product.count()}")
    print(f"dim_store rows: {dim_store.count()}")
    print(f"dim_supplier rows: {dim_supplier.count()}")
    print(f"fact_sales rows: {fact_sales.count()}")

    spark.stop()


if __name__ == "__main__":
    main()