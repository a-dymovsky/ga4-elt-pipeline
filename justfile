set export := true   # expose the variables below as env vars to every recipe

DUCKDB_PATH      := justfile_directory() / "include/warehouse/wh.duckdb"
LANDING_DIR      := justfile_directory() / "include/landing/events"
DBT_PROFILES_DIR := justfile_directory() / "include/dbt"
PY               := justfile_directory() / ".venv/bin/python"
DBT              := justfile_directory() / ".venv/bin/dbt"
default:
    @just --list
setup:
    python3 -m venv .venv
    .venv/bin/pip install -U pip
    .venv/bin/pip install -r requirements.txt
generate date="2024-06-01":
    {{PY}} include/generator/generate_events.py --date {{date}}
load date="2024-06-01":
    {{PY}} include/loader/load_to_duckdb.py --date {{date}}
dbt:
    cd include/dbt && {{DBT}} build
build date="2024-06-01": (generate date) (load date) dbt

query:
    {{PY}} -c "import duckdb, os; con = duckdb.connect(os.environ['DUCKDB_PATH']); print(con.sql('select d.channel_group, count(*) as sessions, sum(s.is_engaged) as engaged, round(sum(s.revenue), 2) as revenue from main.fct_sessions s join main.dim_traffic_source d using (traffic_source_key) group by 1 order by revenue desc'))"

clean:
    rm -rf include/warehouse include/landing include/dbt/target include/dbt/logs include/dbt/dbt_packages
