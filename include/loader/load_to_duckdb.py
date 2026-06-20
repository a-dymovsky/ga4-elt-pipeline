"""Load one date partition of raw events into DuckDB, idempotently.

This is the EL in ELT: extract-and-load is deliberately dumb (land the raw bytes
faithfully); all the shaping happens later in dbt. Re-running a date deletes that
date's rows first, so a backfill or a retried Airflow task never double-counts.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "include/warehouse/wh.duckdb")
LANDING_DIR = os.environ.get("LANDING_DIR", "include/landing/events")


def load_partition(date: str) -> int:
    """Load (or reload) the partition for ``date``. Returns the row count for that date."""
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    all_partitions = f"{LANDING_DIR}/event_date=*/*.parquet"
    this_partition = f"{LANDING_DIR}/event_date={date}/*.parquet"

    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        # First run only: create the table by borrowing the parquet's schema.
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS raw.events AS
            SELECT * FROM read_parquet('{all_partitions}', hive_partitioning = true)
            WHERE 1 = 0
            """
        )
        con.execute("DELETE FROM raw.events WHERE event_date = ?", [date])
        con.execute(
            f"""
            INSERT INTO raw.events
            SELECT * FROM read_parquet('{this_partition}', hive_partitioning = true)
            """
        )
        count = con.execute(
            "SELECT count(*) FROM raw.events WHERE event_date = ?", [date]
        ).fetchone()[0]
    finally:
        con.close()

    print(f"[load] {count:>6} rows for {date} -> raw.events in {DUCKDB_PATH}")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load one date partition into DuckDB.")
    parser.add_argument("--date", required=True, help="event date, YYYY-MM-DD")
    args = parser.parse_args()
    load_partition(args.date)
