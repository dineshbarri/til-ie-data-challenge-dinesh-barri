"""Orchestrate the medallion data pipeline."""

from . import bronze, gold, silver


def run() -> None:
    """Run bronze, silver, and gold processing in order."""
    bronze.run()
    silver.run()
    gold.run()


if __name__ == "__main__":
    run()

