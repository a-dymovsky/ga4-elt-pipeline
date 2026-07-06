# ga4-elt-pipeline

An end-to-end ELT pipeline for GA4-style web-analytics events.
Proposed process: generate → land → load → transform (dbt) → orchestrate (Airflow).
Runs free and locally on DuckDB.

🚧 Work in progress.

## Quickstart

Requires Python 3.12+. Runs entirely locally on DuckDB: no cloud accounts needed.

```bash
# 1. Clone and enter the project.
git clone git@github.com:a-dymovsky/ga4-elt-pipeline.git
cd ga4-elt-pipeline

# 2. Set up the environment.
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Run the whole pipeline: generate → load → transform → test
just build
```

That's it. `just build` generates a day of synthetic events, lands them in DuckDB,
and runs the dbt models with their tests. To see the result:

```bash
just query
```

```text
channel_group    sessions   engaged   revenue
organic search       1356      1118   44267.71
paid search           635       528   20233.78
direct                730       606   19689.89
```
