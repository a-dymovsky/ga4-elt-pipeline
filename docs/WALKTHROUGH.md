# Pipeline Walkthrough

A step-by-step walkthrough of the pipeline. Each section describes each functional area within the pipeline and provides instructions for local implementation, validation, and confirmation. Please see the [README](https://github.com/a-dymovsky/ga4-elt-pipeline/blob/main/README.md) for quickstart instructions.
    
### Step 1: Generate Events

`generate()` produces one day's worth of synthetic GA4-shaped web-analytics events. Each run is reproducible and seeded by the date. Each "user" is captured with one or more sessions, with each session serving as a weighted-but-random sequence of events. Each session is stamped with a traffic source and every event with an increasing timestamp. Each event is assembled into a flat record with additional details packaged into a JSON ```event_params``` field to mirror GA4's BigQuery export. The full day's data is collected into a Pandas dataframe and written as Parquet into a date-partitioned folder (```event_date=YYYY-MM-DD/```) that is overwritten. Each date run replaces its data.

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

### Step 2

`load_partition()` copies a single date's Parquet yield into DuckDB as a `raw.events` table. This represents the "extract" and "load" portions of this ELT project. Raw data is expressed in the JSON 'event_params' for dbt to handle downstream. The load is idempotent: any re-attempts or backfills never produce duplicates. Hive partitioning means 'event_date' is derived from the folder name rather than read from the file itself.

### Proof

```bash
just generate 2024-06-01
just load 2024-06-01
```

Confirm the events landed as rows in the database:

```bash
python -c "import duckdb; con = duckdb.connect('include/warehouse/wh.duckdb'); print(con.sql('select count(*) as total from raw.events'))"
```

Confirm idempotent returns by loading the same data twice. Confirm that the count does not double.

```bash
just load 2024-06-01
python -c "import duckdb; con = duckdb.connect('include/warehouse/wh.duckdb'); print(con.sql('select count(*) as total_after_reload from raw.events'))"
```
