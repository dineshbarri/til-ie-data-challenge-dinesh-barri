
from pathlib import Path
from pyspark.sql import functions as F

from src.spark_session import create_spark

DATA = Path("src/data")
SILVER = DATA / "silver"
GOLD = DATA / "gold"

# Diagnostic threshold only, not an approved business rule.
LARGE_REBATE_THRESHOLD = 1000000


def save_table(df, name):
    path = GOLD / name

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )

    print(f"Saved: {name}")


def build_gold(spark):
    sales = spark.read.format("delta").load(
        str(SILVER / "silver_sales")
    )

    rebates = spark.read.format("delta").load(
        str(SILVER / "silver_rebates")
    )

    invalid_sales = spark.read.format("delta").load(
        str(DATA / "quarantine" / "invalid_sales")
    )

    # Validate the sales grain before any financial joins.
    if sales.groupBy("sale_transaction_id").count() \
            .filter("count > 1").limit(1).count():
        raise ValueError("Duplicate valid sale IDs found")

    if rebates.groupBy("rebate_transaction_id").count() \
            .filter("count > 1").limit(1).count():
        raise ValueError("Duplicate rebate IDs found")

    sale_lookup = sales.select(
        "sale_transaction_id",
        F.col("sale_amount").alias("matched_sale_amount"),
        F.col("currency_code").alias("sale_currency")
    )

    invalid_ids = invalid_sales.select(
        "sale_transaction_id"
    ).distinct().withColumn(
        "is_quarantined_sale", F.lit(True)
    )

    # Every rebate is retained, including unmatched rebates.
    audit = (
        rebates
        .join(sale_lookup, "sale_transaction_id", "left")
        .join(invalid_ids, "sale_transaction_id", "left")
        .withColumn(
            "exceeds_sale",
            F.when(
                F.col("matched_sale_amount").isNotNull(),
                F.col("rebate_amount")
                > F.col("matched_sale_amount")
            ).otherwise(False)
        )
        .withColumn(
            "over_one_million",
            F.col("rebate_amount")
            > F.lit(LARGE_REBATE_THRESHOLD)
        )
        .withColumn(
            "currency_mismatch",
            F.when(
                F.col("sale_currency").isNotNull(),
                F.col("currency_code")
                != F.col("sale_currency")
            ).otherwise(False)
        )
        .withColumn(
            "reconciliation_status",
            F.when(
                F.col("matched_sale_amount").isNull()
                & F.coalesce(
                    F.col("is_quarantined_sale"),
                    F.lit(False)
                ),
                "UNMATCHED_QUARANTINED_SALE"
            )
            .when(
                F.col("matched_sale_amount").isNull(),
                "UNMATCHED_UNKNOWN_SALE"
            )
            .when(
                F.col("currency_mismatch"),
                "MATCHED_CURRENCY_MISMATCH"
            )
            .when(
                F.col("exceeds_sale")
                | F.col("over_one_million"),
                "MATCHED_FLAGGED"
            )
            .otherwise("MATCHED_UNFLAGGED")
        )
    )

    # Count the reconciliation statuses before saving.
    print("\nREBATE RECONCILIATION STATUS")
    audit.groupBy("reconciliation_status").count().show(
        truncate=False
    )

    if audit.count() != rebates.count():
        raise ValueError("Rebate row-count reconciliation failed")

    if audit.filter("currency_mismatch").count() > 0:
        raise ValueError(
            "Currency mismatches must be resolved before Gold"
        )

    # Aggregate rebates to sale grain before joining.
    rebate_totals = (
        audit
        .filter(
            F.col("reconciliation_status").isin(
                "MATCHED_UNFLAGGED",
                "MATCHED_FLAGGED"
            )
        )
        .groupBy("sale_transaction_id")
        .agg(
            F.sum("rebate_amount").alias(
                "all_matched_rebates"
            ),
            F.sum(
                F.when(
                    F.col("reconciliation_status")
                    == "MATCHED_UNFLAGGED",
                    F.col("rebate_amount")
                ).otherwise(F.lit(0))
            ).alias("unflagged_rebates"),
            F.sum(
                F.when(
                    F.col("reconciliation_status")
                    == "MATCHED_FLAGGED",
                    F.col("rebate_amount")
                ).otherwise(F.lit(0))
            ).alias("flagged_rebates"),
            F.count("*").alias("rebate_count")
        )
    )

    zero = F.lit(0).cast("decimal(18,2)")

    fact = (
        sales
        .join(rebate_totals, "sale_transaction_id", "left")
        .withColumn(
            "all_matched_rebates",
            F.coalesce("all_matched_rebates", zero)
        )
        .withColumn(
            "unflagged_rebates",
            F.coalesce("unflagged_rebates", zero)
        )
        .withColumn(
            "flagged_rebates",
            F.coalesce("flagged_rebates", zero)
        )
        .withColumn(
            "rebate_count",
            F.coalesce("rebate_count", F.lit(0))
        )
        .withColumn(
            "source_basis_net_revenue",
            F.col("sale_amount")
            - F.col("all_matched_rebates")
        )
        .withColumn(
            "provisional_net_revenue",
            F.col("sale_amount")
            - F.col("unflagged_rebates")
        )
        .withColumn(
            "sale_amount_over_one_million",
            F.col("sale_amount") > F.lit(1000000)
        )
        .withColumn(
            "has_flagged_rebate",
            F.col("flagged_rebates") != 0
        )
    )

    if fact.count() != sales.count():
        raise ValueError("Sales grain changed during join")

    # Financial control: all matched rebates must reconcile.
    controls = fact.groupBy("currency_code").agg(
        F.sum("sale_amount").alias("gross_sales"),
        F.sum("all_matched_rebates").alias("rebates"),
        F.sum("source_basis_net_revenue").alias("net")
    )

    failed = controls.filter(
        F.abs(
            F.col("gross_sales")
            - F.col("rebates")
            - F.col("net")
        ) > F.lit(0.01)
    )

    if failed.count() > 0:
        raise ValueError("Financial control failed")

    # Dimension tables.
    dim_region = (
        sales.select(
            F.col("customer_region").alias("region")
        )
        .distinct()
    )

    # Use rep ID as the dimension business key.
    # Preserve potential name inconsistencies for later review.
    dim_sales_rep = (
        sales.groupBy("sales_rep_id")
        .agg(
            F.first(
                "sales_rep_name", ignorenulls=True
            ).alias("sales_rep_name"),
            F.countDistinct(
                "sales_rep_name"
            ).alias("distinct_name_count")
        )
    )

    # Save only after validations have passed.
    save_table(audit, "fact_rebate_audit")
    save_table(fact, "fact_sales_reconciled")
    save_table(dim_region, "dim_region")
    save_table(dim_sales_rep, "dim_sales_rep")

    print("\nGOLD PROCESSING COMPLETE")
    print("Sales fact rows:", fact.count())
    print("Rebate audit rows:", audit.count())

    print("\nFINANCIAL CONTROL BY CURRENCY")
    controls.orderBy("currency_code").show(
        truncate=False
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        build_gold(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
