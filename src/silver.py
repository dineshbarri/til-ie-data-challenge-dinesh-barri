
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import create_spark


DATA_PATH = Path("src/data")
BRONZE = DATA_PATH / "bronze"
SILVER = DATA_PATH / "silver"
QUARANTINE = DATA_PATH / "quarantine"


SALES_COLUMNS = {
    "Sale Transaction ID": "sale_transaction_id",
    "Sales Rep ID": "sales_rep_id",
    "Sales Rep Name": "sales_rep_name",
    "Sales Rep Country": "sales_rep_country",
    "Sales Rep Country Code": "sales_rep_country_code",
    "Sales Rep Region": "sales_rep_region",
    "Sales Country Code": "sales_country_code",
    "Customer ID": "customer_id",
    "Customer Name": "customer_name",
    "Customer Country": "customer_country",
    "Customer Region": "customer_region",
    "Customer Country Code": "customer_country_code",
    "Unit Quantity": "unit_quantity",
    "Sale Amount": "sale_amount",
    "Sale Datetime": "sale_datetime",
    "Product Details": "product_details",
    "Product Code": "product_code",
    "Sale Location": "sale_location",
    "CurrencyCode": "currency_code",
}

REBATE_COLUMNS = {
    "Sales Rep ID": "sales_rep_id",
    "Sales Rep Name": "sales_rep_name",
    "Sales Rep Region": "sales_rep_region",
    "Customer Country": "customer_country",
    "Customer Region": "customer_region",
    "Sale Location": "sale_location",
    "CurrencyCode": "currency_code",
    "Customer Name": "customer_name",
    "Sale Transaction ID": "sale_transaction_id",
    "Customer ID": "customer_id",
    "Rebate Datetime": "rebate_datetime",
    "Rebate Transaction ID": "rebate_transaction_id",
    "Rebate Type": "rebate_type",
    "Rebate Amount": "rebate_amount",
}


def rename_columns(df, mapping):
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    return df


def clean_strings(df):
    for column, dtype in df.dtypes:
        if dtype == "string" and column not in [
            "source_file", "_corrupt_record"
        ]:
            df = df.withColumn(
                column,
                F.trim(F.col(column))
            )
    return df


def parse_datetime(column):
    return F.coalesce(
        F.to_timestamp(column, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(column, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(column, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(column, "yyyy-MM-dd"),
        F.to_timestamp(column, "dd/MM/yyyy"),
    )


def save_delta(df, path):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def process_sales(spark):
    print("\nProcessing Silver Sales...")

    df = spark.read.format("delta").load(
        str(BRONZE / "bronze_sales")
    )

    df = rename_columns(df, SALES_COLUMNS)
    df = clean_strings(df)

    df = (
        df
        .withColumn(
            "sale_amount",
            F.expr(
                "try_cast(sale_amount as decimal(18,2))"
            )
        )
        .withColumn(
            "unit_quantity",
            F.expr("try_cast(unit_quantity as integer)")
        )
        .withColumn(
            "sale_timestamp",
            parse_datetime("sale_datetime")
        )
        .withColumn(
            "currency_code",
            F.upper(F.col("currency_code"))
        )
    )

    invalid = (
        F.col("_corrupt_record").isNotNull()
        | F.col("sale_transaction_id").isNull()
        | (F.col("sale_transaction_id") == "")
        | F.col("sale_amount").isNull()
        | F.col("sale_timestamp").isNull()
        | F.col("unit_quantity").isNull()
        | (F.col("unit_quantity") <= 0)
        | F.col("currency_code").isNull()
        | (F.col("currency_code") == "")
    )

    quarantine = df.filter(invalid).withColumn(
        "quality_issue",
        F.lit("INVALID_SALES_RECORD")
    )

    valid = df.filter(~invalid)

    save_delta(
        quarantine,
        QUARANTINE / "invalid_sales"
    )

    # Keep unique transaction IDs only.
    duplicates = (
        valid.groupBy("sale_transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .select("sale_transaction_id")
    )

    duplicate_records = (
        valid.join(
            duplicates,
            "sale_transaction_id",
            "inner"
        )
        .withColumn(
            "quality_issue",
            F.lit("DUPLICATE_SALE_TRANSACTION_ID")
        )
    )

    save_delta(
        duplicate_records,
        QUARANTINE / "duplicate_sales"
    )

    clean = valid.join(
        duplicates,
        "sale_transaction_id",
        "left_anti"
    )

    save_delta(
        clean,
        SILVER / "silver_sales"
    )

    print("Silver Sales:", clean.count())
    print("Invalid Sales:", quarantine.count())
    print("Duplicate Sales:", duplicate_records.count())


def process_rebates(spark):
    print("\nProcessing Silver Rebates...")

    df = spark.read.format("delta").load(
        str(BRONZE / "bronze_rebates")
    )

    df = rename_columns(df, REBATE_COLUMNS)
    df = clean_strings(df)

    df = (
        df
        .withColumn(
            "rebate_amount",
            F.expr(
                "try_cast(rebate_amount as decimal(18,2))"
            )
        )
        .withColumn(
            "rebate_timestamp",
            parse_datetime("rebate_datetime")
        )
        .withColumn(
            "currency_code",
            F.upper(F.col("currency_code"))
        )
    )

    invalid = (
        F.col("_corrupt_record").isNotNull()
        | F.col("rebate_transaction_id").isNull()
        | (F.col("rebate_transaction_id") == "")
        | F.col("sale_transaction_id").isNull()
        | (F.col("sale_transaction_id") == "")
        | F.col("rebate_amount").isNull()
        | F.col("rebate_timestamp").isNull()
        | F.col("currency_code").isNull()
        | (F.col("currency_code") == "")
    )

    quarantine = df.filter(invalid).withColumn(
        "quality_issue",
        F.lit("INVALID_REBATE_RECORD")
    )

    valid = df.filter(~invalid)

    save_delta(
        quarantine,
        QUARANTINE / "invalid_rebates"
    )

    duplicates = (
        valid.groupBy("rebate_transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .select("rebate_transaction_id")
    )

    duplicate_records = (
        valid.join(
            duplicates,
            "rebate_transaction_id",
            "inner"
        )
        .withColumn(
            "quality_issue",
            F.lit("DUPLICATE_REBATE_TRANSACTION_ID")
        )
    )

    save_delta(
        duplicate_records,
        QUARANTINE / "duplicate_rebates"
    )

    clean = valid.join(
        duplicates,
        "rebate_transaction_id",
        "left_anti"
    )

    save_delta(
        clean,
        SILVER / "silver_rebates"
    )

    print("Silver Rebates:", clean.count())
    print("Invalid Rebates:", quarantine.count())
    print("Duplicate Rebates:", duplicate_records.count())


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        process_sales(spark)
        process_rebates(spark)

        print("\nSILVER PROCESSING COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
