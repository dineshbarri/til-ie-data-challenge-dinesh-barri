
import pytest
from pyspark.sql import functions as F
from src.spark_session import create_spark


@pytest.fixture(scope="module")
def spark():
    session = create_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def read_delta(spark, name):
    return spark.read.format("delta").load(
        f"src/data/gold/{name}"
    )


def test_sales_row_count(spark):
    sales = read_delta(spark, "fact_sales_reconciled")
    assert sales.count() == 26865


def test_rebate_row_count(spark):
    rebates = read_delta(spark, "fact_rebate_audit")
    assert rebates.count() == 2700


def test_reconciliation_status_counts(spark):
    rebates = read_delta(spark, "fact_rebate_audit")

    actual = {
        row["reconciliation_status"]: row["count"]
        for row in rebates.groupBy(
            "reconciliation_status"
        ).count().collect()
    }

    assert actual == {
        "MATCHED_UNFLAGGED": 2601,
        "MATCHED_FLAGGED": 72,
        "UNMATCHED_QUARANTINED_SALE": 27,
    }


def test_sales_transaction_uniqueness(spark):
    sales = read_delta(spark, "fact_sales_reconciled")

    duplicates = (
        sales.groupBy("sale_transaction_id")
        .count()
        .filter(F.col("count") > 1)
    )

    assert duplicates.count() == 0


def test_financial_equation(spark):
    sales = read_delta(spark, "fact_sales_reconciled")

    failures = sales.filter(
        F.abs(
            F.col("sale_amount")
            - F.col("all_matched_rebates")
            - F.col("source_basis_net_revenue")
        ) > F.lit(0.01)
    )

    assert failures.count() == 0


def test_large_sales_count(spark):
    sales = read_delta(spark, "fact_sales_reconciled")

    count = sales.filter(
        F.col("sale_amount_over_one_million")
    ).count()

    assert count == 135


def test_rebate_classification(spark):
    rebates = read_delta(spark, "fact_rebate_audit")

    flagged = rebates.filter(
        F.col("reconciliation_status")
        == "MATCHED_FLAGGED"
    )

    assert flagged.count() == 72

    invalid_flags = flagged.filter(
        ~(F.col("exceeds_sale") | F.col("over_one_million"))
    )

    assert invalid_flags.count() == 0


def test_currency_consistency(spark):
    rebates = read_delta(spark, "fact_rebate_audit")

    mismatches = rebates.filter(
        F.col("currency_mismatch")
    )

    assert mismatches.count() == 0


def test_scenario_financial_equation(spark):
    sales = read_delta(spark, "fact_sales_reconciled")

    eligible = sales.filter(
        F.col("sale_amount") <= 1000000
    )

    failures = eligible.filter(
        F.abs(
            F.col("sale_amount")
            - F.col("unflagged_rebates")
            - F.col("provisional_net_revenue")
        ) > F.lit(0.01)
    )

    assert failures.count() == 0
