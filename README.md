
# Retail Sales & Rebates Reconciliation
### Data Engineering Technical Challenge | The Information Lab Ireland

A reproducible Data Engineering Proof of Concept (POC) that
ingests retail sales and rebate data from Azure Blob Storage,
validates data quality, reconciles transactions, and produces
business-ready analytical outputs using PySpark and Delta Lake.

## 1. Executive Summary

The client needs a reliable view of sales performance after
rebates, segmented by region and sales representative.

The POC implements a Medallion Architecture (Bronze, Silver,
Gold) and identifies material data quality and financial
reconciliation issues.

### Key Findings

| Metric | Result |
|---|---:|
| Source CSV files | 36 |
| Raw sales records | 27,000 |
| Raw rebate records | 2,700 |
| Valid Silver sales | 26,865 |
| Negative-quantity sales quarantined | 135 |
| Matched rebate records | 2,673 |
| Rebates linked to quarantined sales | 27 |
| Matched rebates flagged for review | 72 |
| Sales above 1 million (local currency) | 135 |

All 2,700 rebates are accounted for:
- 2,601 matched and unflagged
- 72 matched but flagged for financial review
- 27 linked to quarantined sales

The most significant finding is that 135 unusually large
sales records account for virtually all source-basis gross
sales value across the five currencies.

These transactions require business validation before
reported revenue can be considered reliable.

## 2. Technology Stack

- Python 3.10
- PySpark 3.5.0
- Delta Lake 3.0.0
- Azure Blob Storage SDK
- GitHub Codespaces
- pytest

The solution runs in a local Spark environment configured
by the challenge template.

The architecture is designed to be portable to a Microsoft
Fabric Lakehouse, but this POC has not been deployed to
Microsoft Fabric.

## 3. Architecture

Azure Blob Storage
        |
        v
Python Azure SDK Ingestion
        |
        v
Local Raw CSV Files
        |
        v
BRONZE - Delta Lake
- Preserve original values
- Source file lineage
- Ingestion timestamp
- Batch identifier
        |
        v
SILVER - Delta Lake
- Standardize column names
- Convert dates and amounts
- Validate required fields
- Detect duplicate transaction IDs
- Quarantine invalid records
        |
        v
GOLD - Delta Lake
- Reconcile sales and rebates
- Classify financial exceptions
- Calculate revenue measures
- Build analytical dimensions
        |
        v
Business Analysis
- Regional sales performance
- Sales representative performance
- Rebate reconciliation
- Financial outlier impact

### Bronze

Bronze stores source records with ingestion metadata.

The current implementation performs a full refresh and
replaces the previous Bronze snapshot.

### Silver

Silver applies validation rules and creates separate
quarantine tables.

The pipeline currently quarantines nonpositive unit
quantities. The 135 negative-quantity records may represent
returns and require business confirmation.

### Gold

Gold creates:
- fact_sales_reconciled
- fact_rebate_audit
- dim_region
- dim_sales_rep

The reconciled sales fact has one row per valid sale.

Rebates are aggregated to the sales transaction grain
before joining, preventing sales multiplication.

The rebate audit preserves matched, flagged, and unmatched
records.

## 4. Repository Structure

```text
src/
  ingest.py
  spark_session.py
  bronze.py
  silver.py
  gold.py
  analysis.py
  outlier_analysis.py
  data/                  # Local data; excluded from Git

tests/
  test_financial_controls.py

outputs/
  analysis/              # Generated locally; excluded from Git

presentation/
  head_of_sales.pdf

README.md
requirements.txt
.gitignore
```

## 5. Setup

### Prerequisites

Use the GitHub Codespaces environment supplied by the
challenge template.

Verify the dependencies:

```bash
python --version
java -version
python -m pip check
```

### Configure Azure access

The Azure SAS token must be provided securely through the
AZURE_STORAGE_SAS_TOKEN environment variable.

Do not commit credentials to Git.

For a temporary terminal session:

```bash
read -rs AZURE_STORAGE_SAS_TOKEN
export AZURE_STORAGE_SAS_TOKEN
```

For persistent access, use a GitHub Codespaces secret.

### Run ingestion

```bash
python src/ingest.py
```

The script downloads sales and rebate CSV files into
src/data/raw/.

### Run Bronze

```bash
python -m src.bronze
```

### Run Silver

```bash
python -m src.silver
```

### Run Gold

```bash
python -m src.gold
```

### Generate business analysis

```bash
python -m src.analysis
```

### Generate outlier impact analysis

```bash
python -m src.outlier_analysis
```

### Run tests

```bash
python -m pytest tests/ -v
```

Run the commands from the repository root.

## 6. Data Quality Findings

### Negative Sales Quantities

135 of 27,000 sales records (0.50%) have negative
unit quantities.

These records were quarantined rather than deleted.

Negative quantities may represent customer returns,
adjustments, or source-data issues.

The correct business treatment remains to be confirmed.

### Rebate Reconciliation

Of 2,700 rebate records:

- 2,673 match valid sales
- 27 match quarantined sales
- 0 reference completely unknown sales
- 0 matched records have currency mismatches

### Financial Anomalies

Among matched rebates:
- 45 exceed their associated sale amounts
- 27 exceed 1 million in their local currency
- The two groups do not overlap

There are 72 flagged matched rebate records.

Additionally, 135 valid sales exceed 1 million in
their local currency.

These thresholds are diagnostic flags, not approved
business rules or proof of incorrect data.

## 7. Financial Measures

Source-basis net revenue:

    Gross Sales - All Matched Rebates

Provisional net revenue:

    Gross Sales - Unflagged Matched Rebates

Outlier-excluded scenario:

    Sales with amount <= 1,000,000
    minus unflagged rebates associated with those sales

The outlier-excluded scenario also excludes the rebates
associated with excluded large sales.

All measures are calculated separately by currency.

The source-basis and provisional measures are not certified
financial results.

## 8. Regional Analysis

In the exploratory outlier-excluded scenario:

| Currency | EMEA Net | North America Net |
|---|---:|---:|
| EUR | 815,965.88 | 488,172.57 |
| USD | 203,470.58 | 127,488.26 |
| GBP | 201,970.45 | 123,960.82 |
| CAD | 203,281.86 | 124,117.39 |
| MXN | 193,145.19 | 117,170.78 |

EMEA has higher scenario net revenue in each reported
currency.

These results are exploratory and should not be interpreted
as certified revenue or profitability.

Currency amounts must not be combined without an agreed
foreign-exchange conversion methodology.

## 9. Business Assumptions

1. Sale Transaction ID is the relationship key between
   sales and rebates.

2. Rebate Transaction ID uniquely identifies a rebate.

3. A rebate is associated with the referenced sale.

4. Rebate amounts are treated as deductions when calculating
   source-basis net revenue.

5. Negative unit quantities remain quarantined until the
   client confirms whether they represent returns.

6. Amounts above 1 million are flagged for investigation,
   not automatically classified as errors.

7. Rebates exceeding sale amounts require review.

8. Customer Region is used for geographic revenue reporting.

9. Each currency is reported separately.

10. The outlier-excluded results are analytical scenarios,
    not approved accounting adjustments.

## 10. Data Quality and Financial Controls

The implementation checks:

- Required identifiers and values
- Parseable dates and numeric amounts
- Duplicate transaction identifiers
- Sales-to-rebates relationships
- Currency mismatches
- Rebate exception statuses
- Sales grain preservation
- Gross sales less rebates equals net revenue

The integration tests verify selected Gold outputs.

## 11. Current Limitations

- Full-refresh ingestion rather than incremental loading
- Local Codespaces execution rather than Fabric deployment
- Financial outlier thresholds are exploratory
- Negative quantities require business clarification
- No approved multi-currency conversion policy
- No production scheduling or alerting
- Dimension attributes do not implement historical tracking
- Source-level business definitions require confirmation

## 12. Production Roadmap

### Phase 1: Business Validation

- Review large sales and rebate transactions with Finance
- Confirm the meaning of negative quantities
- Agree rebate allocation and reversal rules
- Define certified financial metrics

### Phase 2: Engineering Hardening

- Incremental ingestion and source file tracking
- Automated data quality monitoring
- CI/CD and expanded regression tests
- Structured logging and pipeline observability
- Improved schema evolution and error handling

### Phase 3: Microsoft Fabric Deployment

- Deploy Delta tables to a Fabric Lakehouse
- Orchestrate ingestion and transformations
- Implement security and governance
- Publish a certified semantic model
- Build Power BI reporting for business users

## 13. Presentation

The five-slide Head of Sales presentation summarizes:

1. Executive findings
2. Financial anomalies and reconciliation
3. Regional performance
4. Data engineering architecture
5. Business recommendations and roadmap

## 14. Conclusion

The POC demonstrates that reliable business reporting
requires more than joining sales and rebate datasets.

It requires source traceability, data quality controls,
correct transaction-grain reconciliation, financial
exception management, and transparent reporting assumptions.

The solution provides a reproducible foundation for
developing a governed retail revenue data product.
```
