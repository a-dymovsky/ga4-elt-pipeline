"""Synthetic GA4 event generator.

Emits GA4-style events for a single date as Hive-partitioned Parquet, mirroring
the shape of Google's GA4 BigQuery export: one row per event, with the noisy
per-event payload held in a JSON ``event_params`` column. The point is a
*controllable* source -- change the date, the volume, or the seed and you can
simulate traffic, backfills, and re-runs without touching a real warehouse.

Idempotent by date: running twice for the same date overwrites that date's
partition rather than appending, so re-runs are safe.
"""

from __future__ import annotations
import argparse
import json
import os
import random
import shutil
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

LANDING_DIR = os.environ.get("LANDING_DIR", "include/landing/events")

# See "BigQuery Export Schema" at https://support.google.com/analytics/answer/7029846?hl=en
# Events below are a very simplified version of the items denoted above.
# (source, medium, campaign, weight): synthetic targets based off typical UTM comparables.
# Weights are completely arbitrary.

TRAFFIC_SOURCES = [
    ("google", "organic", "(not set)", 0.34),
    ("google", "cpc", "spring_sale", 0.18),
    ("(direct)", "(none)", "(not set)", 0.22),
    ("newsletter", "email", "weekly_digest", 0.10),
    ("facebook", "social", "brand_awareness", 0.09),
    ("bing", "organic", "(not set)", 0.07),
]

PAGES = [
    "/", "/catalog", "/product/standing-desk", "/product/monitor-arm",
    "/cart", "/checkout", "/about", "/blog/wfh-setup",
]

DEVICES = [
    ("desktop", "macOS", "Chrome"), ("mobile", "iOS", "Safari"),
    ("desktop", "Windows", "Edge"), ("mobile", "Android", "Chrome"),
]

# Arbitrary countries.

COUNTRIES = ["United States", "Canada", "United Kingdom", "Germany", "India"]


def _weighted_source():
    choices = [row[:3] for row in TRAFFIC_SOURCES]
    weights = [row[3] for row in TRAFFIC_SOURCES]
    return random.choices(choices, weights = weights)[0]


def generate(date: str, n_users: int = 800, seed: int | None = None) -> Path:
    # Date-derived seed so the same date reproduces identical data (idempotent
    # re-runs); pass --seed to vary it.
    random.seed(f"{date}-{seed}" if seed is not None else date)
    day_start = datetime.fromisoformat(date).replace(tzinfo = timezone.utc).timestamp()

    rows: list[dict] = []
    session_counter = 0
    for user_idx in range(n_users):
        user_pseudo_id = f"u{user_idx:06d}.{random.randint(10**9, 10**10)}"
        device_category, operating_system, browser = random.choice(DEVICES)
        country = random.choice(COUNTRIES)

        n_sessions = random.choices([1, 2, 3], weights=[0.70, 0.22, 0.08])[0]
        for _ in range(n_sessions):
            session_counter += 1
            ga_session_id = int(day_start) + session_counter
            source, medium, campaign = _weighted_source()

            n_events = random.choices(
                [1, 2, 3, 5, 8, 12], weights=[0.18, 0.22, 0.20, 0.18, 0.14, 0.08]
            )[0]
            is_engaged = 1 if n_events >= 2 else 0

            event_names = ["session_start", "page_view"]
            for _ in range(max(0, n_events - 1)):
                event_names.append(
                    random.choices(
                        ["page_view", "scroll", "add_to_cart", "begin_checkout", "purchase"],
                        weights=[0.55, 0.20, 0.13, 0.08, 0.04],
                    )[0]
                )

            seconds = random.randint(0, 86_400 - 600)
            for event_name in event_names:
                seconds += random.randint(2, 90)
                page = random.choice(PAGES)
                params = {
                    "page_location": f"https://shop.example.com{page}",
                    "page_title": page.strip("/").replace("/", " ") or "home",
                    "ga_session_id": ga_session_id,
                    "session_engaged": is_engaged,
                }

                if event_name == "purchase":
                    params["value"] = round(random.uniform(29, 480), 2)
                    params["currency"] = "USD"

                rows.append(
                    {
                        "event_date": date,
                        "event_timestamp": int((day_start + seconds) * 1_000_000),
                        "event_name": event_name,
                        "user_pseudo_id": user_pseudo_id,
                        "ga_session_id": ga_session_id,
                        "session_engaged": is_engaged,
                        "device_category": device_category,
                        "operating_system": operating_system,
                        "browser": browser,
                        "country": country,
                        "traffic_source_source": source,
                        "traffic_source_medium": medium,
                        "traffic_source_campaign": campaign,
                        "event_params": json.dumps(params),
                    }
                )

    frame = pd.DataFrame(rows)
    partition_dir = Path(LANDING_DIR) / f"event_date={date}"
    if partition_dir.exists():
        shutil.rmtree(partition_dir)  # idempotent overwrite of this date only
    partition_dir.mkdir(parents=True)
    # event_date dropped from file and lives in the path consistent with Hive partitioning.
    frame.drop(columns=["event_date"]).to_parquet(partition_dir / "part-0.parquet", index=False)
    print(f"[generate] {len(frame):>6} events -> {partition_dir}")
    return partition_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic GA4 events for one date.")
    parser.add_argument("--date", required=True, help="event date, YYYY-MM-DD")
    parser.add_argument("--n-users", type=int, default=800)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    generate(args.date, args.n_users, args.seed)
