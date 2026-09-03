# Pipeline Walkthrough

A step-by-step walkthrough of the pipeline. Each section describes each functional area within the pipeline and provides instructions for local implementation, validation, and confirmation. Please see the [README](https://github.com/a-dymovsky/ga4-elt-pipeline/blob/main/README.md) for quickstart instructions.
    
## Step 1: Generate Events

`generate()` produces one day's worth of synthetic GA4-shaped web-analytics events. Each run is reproducible and seeded by the date. Each "user" is captured with one or more sessions, with each session serving as a weighted-but-random sequence of events. Each session is stamped with a traffic source and every event with an increasing timestamp. Each event is assembled into a flat record with additional details packaged into a JSON `event_params` field to mirror GA4's BigQuery export. The full day's data is collected into a Pandas dataframe and written as Parquet into a date-partitioned folder (`event_date=YYYY-MM-DD/`) that is overwritten. Each date run replaces its data.

### Proof

```bash
cd ~/ga4-elt-pipeline
source .venv/bin/activate
```

Generate a day's data:

```bash
python include/generator/generate_events.py --date 2024-06-01
```

```text
[generate]   5759 events -> include/landing/events/event_date=2024-06-01 
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

```text
[generate]   5759 events -> include/landing/events/event_date=2024-06-01 
run 1 rows: 5759  
run 1 hash: 32dd86bd1c73cedff5b0571bee0eb00c 
[generate]   5759 events -> include/landing/events/event_date=2024-06-01  
run 2 rows: 5759 
run 2 hash: 32dd86bd1c73cedff5b0571bee0eb00c 
```

Both runs produce the same hash. Event generation is provably deterministic. Any subsequent run, or potential backfill, produces identical data.

## Step 2: Extract and Load Events

`load_partition()` copies a single date's Parquet yield into DuckDB as a `raw.events` table. This represents the "extract" and "load" portions of this ELT project. Raw data is expressed in the JSON 'event_params' for dbt to handle downstream. The load is idempotent: any re-attempts or backfills never produce duplicates. Hive partitioning means 'event_date' is derived from the folder name rather than read from the file itself.

### Proof

```bash
just generate 2024-06-01
just load 2024-06-01
```

```text
[generate]   5759 events -> /home/personal/Documents/ga4-elt-pipeline/include/landing/events/event_date=2024-06-01
```

```text
[load]   5759 rows for 2024-06-01 -> raw.events in /home/personal/Documents/ga4-elt-pipeline/include/warehouse/wh.duckdb 
```

Confirm the events landed as rows in the database:

```bash
python -c "import duckdb; con = duckdb.connect('include/warehouse/wh.duckdb'); print(con.sql('select count(*) as total from raw.events'))"
```

```text
┌───────┐
│ total │
│ int64 │
├───────┤
│  5759 │
└───────┘
```

Confirm idempotent returns by loading the same data twice. Confirm that the count does not double.

```bash
just load 2024-06-01
python -c "import duckdb; con = duckdb.connect('include/warehouse/wh.duckdb'); print(con.sql('select count(*) as total_after_reload from raw.events'))"
```

```text
/home/personal/Documents/ga4-elt-pipeline/.venv/bin/python include/loader/load_to_duckdb.py --date 2024-06-01
[load]   5759 rows for 2024-06-01 -> raw.events in /home/personal/Documents/ga4-elt-pipeline/include/warehouse/wh.duckdb
┌────────────────────┐
│ total_after_reload │
│       int64        │
├────────────────────┤
│               5759 │
└────────────────────┘
```

Event count remains unchanged. The loader deletes extant rows prior to inserting.

## Step 3: Transform (dbt)

dbt reads `raw.events` and builds a modeled layer on top of it inside the same DuckDB file. `stg_events` is a view that parses the JSON `event_params` payload into typed columns and standardizes naming. Three materialized marts sit on top of it: `fct_events` (one row per event with surrogate keys), `fct_sessions` (one row per session for its events and attributes), and `dim_traffic_source` (distinct UTM combinations with a derived `channel_group`). dbt determines build order itself by reading the `ref()` links between models, and `dbt build` runs models and their tests together.

### Proof

Run the models.

```bash
just dbt
```

```text

cd include/dbt && /home/personal/Documents/ga4-elt-pipeline/.venv/bin/dbt build
[0m02:23:01  Running with dbt=1.12.3
[0m02:23:01  Registered adapter: duckdb=1.11.0
[0m02:23:01  Found 4 models, 13 data tests, 1 source, 500 macros
[0m02:23:01  
[0m02:23:01  Concurrency: 4 threads (target='dev')
[0m02:23:01  
[0m02:23:02  1 of 17 START sql view model main.stg_events ................................... [RUN]
[0m02:23:02  1 of 17 OK created sql view model main.stg_events .............................. [[32mOK[0m in 0.10s]
[0m02:23:02  2 of 17 START test accepted_values_stg_events_event_name__session_start__page_view__scroll__add_to_cart__begin_checkout__purchase  [RUN]
[0m02:23:02  3 of 17 START test not_null_stg_events_event_name .............................. [RUN]
[0m02:23:02  4 of 17 START test not_null_stg_events_ga_session_id ........................... [RUN]
[0m02:23:02  5 of 17 START test not_null_stg_events_user_pseudo_id .......................... [RUN]
[0m02:23:02  2 of 17 PASS accepted_values_stg_events_event_name__session_start__page_view__scroll__add_to_cart__begin_checkout__purchase  [[32mPASS[0m in 0.34s]
[0m02:23:02  4 of 17 PASS not_null_stg_events_ga_session_id ................................. [[32mPASS[0m in 0.33s]
[0m02:23:02  3 of 17 PASS not_null_stg_events_event_name .................................... [[32mPASS[0m in 0.34s]
[0m02:23:02  5 of 17 PASS not_null_stg_events_user_pseudo_id ................................ [[32mPASS[0m in 0.32s]
[0m02:23:02  6 of 17 START sql table model main.dim_traffic_source .......................... [RUN]
[0m02:23:02  7 of 17 START sql table model main.fct_events .................................. [RUN]
[0m02:23:02  8 of 17 START sql table model main.fct_sessions ................................ [RUN]
[0m02:23:02  6 of 17 OK created sql table model main.dim_traffic_source ..................... [[32mOK[0m in 0.25s]
[0m02:23:02  9 of 17 START test not_null_dim_traffic_source_traffic_source_key .............. [RUN]
[0m02:23:02  10 of 17 START test unique_dim_traffic_source_traffic_source_key ............... [RUN]
[0m02:23:02  8 of 17 OK created sql table model main.fct_sessions ........................... [[32mOK[0m in 0.28s]
[0m02:23:02  11 of 17 START test not_null_fct_sessions_session_key .......................... [RUN]
[0m02:23:02  7 of 17 OK created sql table model main.fct_events ............................. [[32mOK[0m in 0.29s]
[0m02:23:02  12 of 17 START test relationships_fct_sessions_traffic_source_key__traffic_source_key__ref_dim_traffic_source_  [RUN]
[0m02:23:03  9 of 17 PASS not_null_dim_traffic_source_traffic_source_key .................... [[32mPASS[0m in 0.10s]
[0m02:23:03  10 of 17 PASS unique_dim_traffic_source_traffic_source_key ..................... [[32mPASS[0m in 0.10s]
[0m02:23:03  13 of 17 START test unique_fct_sessions_session_key ............................ [RUN]
[0m02:23:03  14 of 17 START test not_null_fct_events_event_key .............................. [RUN]
[0m02:23:03  11 of 17 PASS not_null_fct_sessions_session_key ................................ [[32mPASS[0m in 0.12s]
[0m02:23:03  15 of 17 START test not_null_fct_events_session_key ............................ [RUN]
[0m02:23:03  12 of 17 PASS relationships_fct_sessions_traffic_source_key__traffic_source_key__ref_dim_traffic_source_  [[32mPASS[0m in 0.13s]
[0m02:23:03  16 of 17 START test relationships_fct_events_traffic_source_key__traffic_source_key__ref_dim_traffic_source_  [RUN]
[0m02:23:03  13 of 17 PASS unique_fct_sessions_session_key .................................. [[32mPASS[0m in 0.18s]
[0m02:23:03  14 of 17 PASS not_null_fct_events_event_key .................................... [[32mPASS[0m in 0.17s]
[0m02:23:03  17 of 17 START test unique_fct_events_event_key ................................ [RUN]
[0m02:23:03  15 of 17 PASS not_null_fct_events_session_key .................................. [[32mPASS[0m in 0.16s]
[0m02:23:03  16 of 17 PASS relationships_fct_events_traffic_source_key__traffic_source_key__ref_dim_traffic_source_  [[32mPASS[0m in 0.14s]
[0m02:23:03  17 of 17 PASS unique_fct_events_event_key ...................................... [[32mPASS[0m in 0.07s]
[0m02:23:03  
[0m02:23:03  Finished running 3 table models, 13 data tests, 1 view model in 0 hours 0 minutes and 1.33 seconds (1.33s).
[0m02:23:03  
[0m02:23:03  [32mCompleted successfully[0m
[0m02:23:03  
[0m02:23:03  Done. PASS=17 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=17
```
