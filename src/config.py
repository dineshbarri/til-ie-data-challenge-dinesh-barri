"""Shared paths and configuration for the data pipeline."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "src" / "data"
RAW_PATH = DATA_ROOT / "raw"
BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver"
GOLD_PATH = DATA_ROOT / "gold"
QUARANTINE_PATH = DATA_ROOT / "quarantine"
ANALYSIS_OUTPUT_PATH = PROJECT_ROOT / "outputs" / "analysis"

