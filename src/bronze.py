
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from src.spark_session import create_spark


RAW_PATH = Path("src/data/raw")
BRONZE_PATH = Path("src/data/bronze")


def ingest_dataset(spark, dataset, batch_id):
    source_folder = RAW_PATH / dataset
    files = sorted(source_folder.rglob("*.csv"))

    if not files:
        raise FileNotFoundError(
            f"No CSV files found in {source_folder}"
        )

    print(f"\nProcessing {dataset}...")
    print(f"Found {len(files)} CSV file(s)")

    # Read the CSV header to discover source columns.
    first_header = (
        spark.read.option("header", True)
        .csv(str(files[0]))
        .columns
    )

    if not first_header:
        raise ValueError(f"Missing header: {files[0]}")

    # Every source column is stored as a string.
    schema = StructType([
        StructField(name, StringType(), True)
        for name in first_header
    ] + [
        StructField("_corrupt_record", StringType(), True)
    ])

    # All files must have the same column structure.
    for file in files:
        columns = (
            spark.read.option("header", True)
            .csv(str(file))
            .columns
        )
        if columns != first_header:
            raise ValueError(
                f"CSV schema mismatch in {file}: {columns}"
            )

    df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv([str(file) for file in files])
        .withColumn(
            "source_file",
            F.input_file_name()
        )
        .withColumn(
            "ingested_at",
            F.current_timestamp()
        )
        .withColumn(
            "ingestion_batch_id",
            F.lit(batch_id)
        )
    )

    target = BRONZE_PATH / f"bronze_{dataset}"

    # Full refresh: replace the previous Bronze snapshot.
    (
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .save(str(target))
)

    saved = spark.read.format("delta").load(str(target))
    count = saved.count()

    print(f"Saved {count} rows to {target}")

    corrupt_count = saved.filter(
        F.col("_corrupt_record").isNotNull()
    ).count()

    print(f"Malformed rows flagged: {corrupt_count}")

    return count


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    batch_id = str(uuid4())

    try:
        sales_count = ingest_dataset(
            spark, "sales", batch_id
        )

        rebates_count = ingest_dataset(
            spark, "rebates", batch_id
        )

        print("\nBRONZE INGESTION COMPLETE")
        print(f"Batch ID: {batch_id}")
        print(f"Sales rows: {sales_count}")
        print(f"Rebate rows: {rebates_count}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
