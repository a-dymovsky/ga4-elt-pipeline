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

### Step 1: Generate Events

```generate()``` produces one day's worth of synthetic GA4-shaped web-analytics events. Each run is reproducible and seeded by the date. Each "user" is captured with one or more sessions, with each session serving as a weighted-but-random sequence of events. Each session is stamped with a traffic source and every event with an increasing timestamp. Each event is assembled into a flat record with additional details packaged into a JSON ```event_params``` field to mirror GA4's BigQuery export. The full day's data is collected into a Pandas dataframe and written as Parquet into a date-partitioned folder (```event_date=YYYY-MM-DD/```) that is overwritten. Each date run replaces its data.

### Proof

```bash
cd ~/ga4-elt-pipeline
source .venv/bin/activate
```

Generate a day's data:

```bash
python include/generator/generate_events.py --date 2024-06-01
```

Inspect the data as a CSV dump:

```bash
python -c "import pandas as pd; pd.read_parquet('include/landing/events/event_date=2024-06-01/part-0.parquet').to_csv('sample_events.csv', index=False)"
```

Validate the seeding. Generate the same date twice and confirm each run is identical:

```bash
python include/generator/generate_events.py --date 2024-06-01
python -c "import pandas as pd; a = pd.read_parquet('include/landing/events/event_date=2024-06-01/part-0.parquet'); print('run 1 rows:', len(a)); import hashlib; print('run 1 hash:', hashlib.md5(a.to_csv(index=False).encode()).hexdigest())"

python include/generator/generate_events.py --date 2024-06-01
python -c "import pandas as pd; a = pd.read_parquet('include/landing/events/event_date=2024-06-01/part-0.parquet'); print('run 2 rows:', len(a)); import hashlib; print('run 2 hash:', hashlib.md5(a.to_csv(index=False).encode()).hexdigest())"
```
