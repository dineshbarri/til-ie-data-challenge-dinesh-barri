
from pathlib import Path
import pandas as pd
import re

# Your project folders
RAW_DIR = Path("src/data/raw")
OUTPUT_DIR = Path("outputs/profiling")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Store summary results
file_results = []
column_results = []

# Find every CSV file
files = sorted(RAW_DIR.rglob("*.csv"))

if not files:
    raise SystemExit(
        "No CSV files found! Run Azure ingestion first."
    )

print("=" * 65)
print("RETAIL DATA ENGINEERING CHALLENGE")
print("SOURCE DATA PROFILING")
print("=" * 65)

print(f"\nTotal CSV files found: {len(files)}")

for path in files:
    print("\n" + "=" * 65)
    print(f"FILE: {path}")
    print("=" * 65)

    # Read everything as text to preserve raw values
    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig"
    )

    # Convert blank strings to missing values
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    rows = len(df)
    columns = len(df.columns)
    file_size = path.stat().st_size

    print(f"Rows: {rows:,}")
    print(f"Columns: {columns}")
    print(f"Size: {file_size / 1024:.2f} KB")

    print("\nCOLUMN NAMES:")
    for col in df.columns:
        print(f"  - {col}")

    # Duplicate analysis
    exact_duplicates = int(df.duplicated().sum())

    print("\nDUPLICATE ANALYSIS:")
    print(f"Exact duplicate rows: {exact_duplicates:,}")

    # Store file-level results
    file_results.append({
        "file": str(path),
        "rows": rows,
        "columns": columns,
        "size_bytes": file_size,
        "exact_duplicates": exact_duplicates
    })

    print("\nCOLUMN QUALITY ANALYSIS:")

    for col in df.columns:
        series = df[col].astype("string").str.strip()

        missing = int(series.isna().sum())
        distinct = int(series.nunique(dropna=True))

        # Detect possible numeric fields
        numeric = pd.to_numeric(
            series.str.replace(",", "", regex=False)
                  .str.replace("€", "", regex=False)
                  .str.replace("$", "", regex=False),
            errors="coerce"
        )

        valid_count = int(series.notna().sum())
        numeric_count = int(numeric.notna().sum())

        numeric_rate = (
            numeric_count / valid_count * 100
            if valid_count else 0
        )

        # Detect potential date fields
        is_date_candidate = bool(
            re.search(
                r"date|timestamp|created|updated",
                col,
                re.IGNORECASE
            )
        )

        invalid_dates = None

        if is_date_candidate:
            parsed_dates = pd.to_datetime(
                series,
                errors="coerce",
                format="mixed"
            )

            invalid_dates = int(
                (series.notna() & parsed_dates.isna()).sum()
            )

        # Detect likely ID columns
        is_id_candidate = bool(
            re.search(
                r"(^id$|_id$|id_|invoice|transaction|order)",
                col,
                re.IGNORECASE
            )
        )

        duplicate_ids = None

        if is_id_candidate:
            duplicate_ids = int(
                series.dropna().duplicated().sum()
            )

        column_results.append({
            "file": str(path),
            "column": col,
            "missing": missing,
            "missing_pct": round(
                missing / rows * 100, 2
            ) if rows else 0,
            "distinct": distinct,
            "numeric_parse_pct": round(numeric_rate, 2),
            "invalid_dates": invalid_dates,
            "duplicate_id_values": duplicate_ids
        })

        print(
            f"\n{col}"
            f"\n  Missing: {missing:,}"
            f"\n  Distinct: {distinct:,}"
            f"\n  Numeric parse rate: {numeric_rate:.1f}%"
        )

        if invalid_dates is not None:
            print(f"  Invalid dates: {invalid_dates:,}")

        if duplicate_ids is not None:
            print(
                f"  Repeated ID values: {duplicate_ids:,}"
            )

    print("\nFINISHED PROFILING:", path.name)

# Save results
file_summary = pd.DataFrame(file_results)
column_summary = pd.DataFrame(column_results)

file_summary.to_csv(
    OUTPUT_DIR / "file_inventory.csv",
    index=False
)

column_summary.to_csv(
    OUTPUT_DIR / "column_quality.csv",
    index=False
)

print("\n" + "=" * 65)
print("PROFILING COMPLETED SUCCESSFULLY")
print("=" * 65)

print("\nResults saved in:")
print(OUTPUT_DIR / "file_inventory.csv")
print(OUTPUT_DIR / "column_quality.csv")

print("\nTOTAL ROWS:", file_summary["rows"].sum())
print("TOTAL FILES:", len(file_summary))
