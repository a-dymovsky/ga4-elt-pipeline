FROM astrocrpublic.azurecr.io/runtime:3.2-5

ENV DUCKDB_PATH=/usr/local/airflow/include/warehouse/wh.duckdb
ENV LANDING_DIR=/usr/local/airflow/include/landing/events
ENV DBT_PROFILES_DIR=/usr/local/airflow/include/dbt
