
from pathlib import Path
from pyspark.sql import functions as F

from src.spark_session import create_spark

GOLD = Path("src/data/gold")
OUTPUT = Path("outputs/analysis")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    OUTPUT.mkdir(parents=True, exist_ok=True)

    try:
        sales = spark.read.format("delta").load(
            str(GOLD / "fact_sales_reconciled")
        )

        rebates = spark.read.format("delta").load(
            str(GOLD / "fact_rebate_audit")
        )

        print("\n========== BUSINESS ANALYSIS ==========")

        # 1. Regional financial performance
        print("\n1. REGIONAL PERFORMANCE")

        regional = (
            sales
            .groupBy("customer_region", "currency_code")
            .agg(
                F.count("*").alias("sales_count"),
                F.sum("sale_amount").alias("gross_sales"),
                F.sum("all_matched_rebates").alias(
                    "matched_rebates"
                ),
                F.sum("flagged_rebates").alias(
                    "flagged_rebates"
                ),
                F.sum("source_basis_net_revenue").alias(
                    "source_basis_net_revenue"
                ),
                F.sum("provisional_net_revenue").alias(
                    "provisional_net_revenue"
                )
            )
            .withColumn(
                "rebate_rate_pct",
                F.when(
                    F.col("gross_sales") != 0,
                    F.round(
                        F.col("matched_rebates")
                        / F.col("gross_sales") * 100,
                        2
                    )
                )
            )
        )

        regional.orderBy(
            "currency_code",
            F.desc("source_basis_net_revenue")
        ).show(100, truncate=False)

        # 2. Sales representative performance
        print("\n2. TOP SALES REPRESENTATIVES")

        reps = (
            sales
            .groupBy(
                "sales_rep_id",
                "sales_rep_name",
                "currency_code"
            )
            .agg(
                F.count("*").alias("sales_count"),
                F.sum("sale_amount").alias("gross_sales"),
                F.sum("all_matched_rebates").alias(
                    "matched_rebates"
                ),
                F.sum("source_basis_net_revenue").alias(
                    "source_basis_net_revenue"
                ),
                F.sum("provisional_net_revenue").alias(
                    "provisional_net_revenue"
                )
            )
        )

        reps.orderBy(
            "currency_code",
            F.desc("source_basis_net_revenue")
        ).show(30, truncate=False)

        # 3. Reconciliation status
        print("\n3. REBATE RECONCILIATION")

        rebate_summary = (
            rebates
            .groupBy(
                "currency_code",
                "reconciliation_status"
            )
            .agg(
                F.count("*").alias("records"),
                F.sum("rebate_amount").alias(
                    "rebate_amount"
                )
            )
        )

        rebate_summary.orderBy(
            "currency_code",
            "reconciliation_status"
        ).show(100, truncate=False)

        # 4. Sales amount anomalies
        print("\n4. SALES ABOVE ONE MILLION")

        large_sales = (
            sales
            .groupBy("currency_code")
            .agg(
                F.count("*").alias("total_sales"),
                F.sum(
                    F.when(
                        F.col("sale_amount_over_one_million"),
                        1
                    ).otherwise(0)
                ).alias("large_sales_count"),
                F.sum(
                    F.when(
                        F.col("sale_amount_over_one_million"),
                        F.col("sale_amount")
                    ).otherwise(F.lit(0))
                ).alias("large_sales_amount"),
                F.sum("sale_amount").alias(
                    "total_gross_sales"
                )
            )
            .withColumn(
                "large_sales_share_pct",
                F.round(
                    F.col("large_sales_amount")
                    / F.col("total_gross_sales") * 100,
                    2
                )
            )
        )

        large_sales.orderBy("currency_code").show(
            truncate=False
        )

        # 5. Rebate exception summary
        print("\n5. REBATE EXCEPTION COUNTS")

        rebates.agg(
            F.count("*").alias("total_rebates"),
            F.sum(
                F.when(
                    F.col("reconciliation_status")
                    == "MATCHED_FLAGGED", 1
                ).otherwise(0)
            ).alias("flagged_matched"),
            F.sum(
                F.when(
                    F.col("reconciliation_status")
                    == "UNMATCHED_QUARANTINED_SALE", 1
                ).otherwise(0)
            ).alias("linked_to_quarantine")
        ).show(truncate=False)

        # Save reports for the presentation.
        reports = {
            "regional_performance": regional,
            "sales_rep_performance": reps,
            "rebate_reconciliation": rebate_summary,
            "large_sales_analysis": large_sales,
        }

        for name, df in reports.items():
            (
                df.coalesce(1)
                .write
                .mode("overwrite")
                .option("header", True)
                .csv(str(OUTPUT / name))
            )

            print(f"Saved report: {name}")

        print("\nBUSINESS ANALYSIS COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
