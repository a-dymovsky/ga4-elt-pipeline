-- Event-grain fact: one row per event, with surrogate keys linking out to the
-- session rollup and the traffic-source dimension.

with events as (
    select * from {{ ref('stg_events') }}
)

select
    md5(
        user_pseudo_id || '-' || cast(ga_session_id as varchar)
        || '-' || cast(event_timestamp as varchar) || '-' || event_name
    ) as event_key,
    md5(user_pseudo_id || '-' || cast(ga_session_id as varchar)) as session_key,
    md5(source || '-' || medium || '-' || campaign) as traffic_source_key,
    event_date,
    event_at,
    event_name,
    user_pseudo_id,
    ga_session_id,
    page_location,
    page_title,
    event_value,
    currency
from events
