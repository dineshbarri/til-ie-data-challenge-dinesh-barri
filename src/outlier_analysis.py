
from pathlib import Path
from pyspark.sql import functions as F
from src.spark_session import create_spark

GOLD = Path("src/data/gold")
OUTPUT = Path("outputs/analysis")

THRESHOLD = 1000000


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        sales = spark.read.format("delta").load(
            str(GOLD / "fact_sales_reconciled")
        )

        print("\n===== OUTLIER IMPACT ANALYSIS =====")

        # Scenario 1: All valid Silver sales.
        source = sales.groupBy(
            "customer_region", "currency_code"
        ).agg(
            F.count("*").alias("source_sales_count"),
            F.sum("sale_amount").alias("source_gross"),
            F.sum("all_matched_rebates").alias(
                "source_rebates"
            ),
            F.sum("source_basis_net_revenue").alias(
                "source_net"
            )
        )

        # Scenario 2: Exclude extreme sales AND
        # exclude flagged rebates on remaining sales.
        eligible = sales.filter(
            F.col("sale_amount") <= THRESHOLD
        )

        scenario = eligible.groupBy(
            "customer_region", "currency_code"
        ).agg(
            F.count("*").alias("scenario_sales_count"),
            F.sum("sale_amount").alias("scenario_gross"),
            F.sum("unflagged_rebates").alias(
                "scenario_rebates"
            ),
            F.sum("provisional_net_revenue").alias(
                "scenario_net"
            ),
            F.sum("flagged_rebates").alias(
                "excluded_flagged_rebates"
            )
        )

        comparison = (
            source.join(
                scenario,
                ["customer_region", "currency_code"],
                "left"
            )
            .fillna(
                0,
                subset=[
                    "scenario_sales_count",
                    "scenario_gross",
                    "scenario_rebates",
                    "scenario_net",
                    "excluded_flagged_rebates",
                ]
            )
            .withColumn(
                "net_difference",
                F.col("source_net")
                - F.col("scenario_net")
            )
        )

        print("\n1. REGIONAL COMPARISON")

        comparison.orderBy(
            "currency_code", "customer_region"
        ).show(100, truncate=False)

        print("\n2. EXCLUDED LARGE SALES")

        sales.filter(
            F.col("sale_amount") > THRESHOLD
        ).groupBy("currency_code").agg(
            F.count("*").alias("excluded_sales"),
            F.sum("sale_amount").alias(
                "excluded_gross"
            ),
            F.sum("all_matched_rebates").alias(
                "rebates_linked_to_excluded_sales"
            )
        ).orderBy("currency_code").show(
            truncate=False
        )

        print("\n3. FLAGGED REBATES ON REMAINING SALES")

        eligible.groupBy("currency_code").agg(
            F.sum("flagged_rebates").alias(
                "flagged_rebates_set_aside"
            )
        ).orderBy("currency_code").show(
            truncate=False
        )

        # Financial control for the scenario.
        failed = comparison.filter(
            F.abs(
                F.col("scenario_gross")
                - F.col("scenario_rebates")
                - F.col("scenario_net")
            ) > F.lit(0.01)
        )

        if failed.count() > 0:
            raise ValueError(
                "Scenario financial reconciliation failed"
            )

        OUTPUT.mkdir(parents=True, exist_ok=True)

        comparison.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(str(OUTPUT / "outlier_comparison"))

        print("\nOUTLIER ANALYSIS COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
