# ga4-elt-pipeline

An end-to-end ELT pipeline for GA4-style web-analytics events:
generate → land → load → transform (dbt) → orchestrate (Airflow).
Runs free and locally on DuckDB; the dbt project is configured to repoint at Snowflake.

🚧 Work in progress.
