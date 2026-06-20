-- Session-grain fact: collapse events to one row per session. This is where
-- attribution lives -- each session is stamped with the traffic source that
-- opened it, plus engagement and conversion measures. arg_min(...) picks the
-- value at the *first* event in the session by timestamp.

with events as (
    select * from {{ ref('stg_events') }}
),

sessions as (
    select
        md5(user_pseudo_id || '-' || cast(ga_session_id as varchar)) as session_key,
        user_pseudo_id,
        ga_session_id,
        min(event_date) as session_date,
        min(event_at)   as session_start_at,
        max(event_at)   as session_end_at,
        count(*)        as event_count,
        max(session_engaged) as is_engaged,
        arg_min(page_location, event_timestamp) as landing_page,
        arg_min(source, event_timestamp)        as source,
        arg_min(medium, event_timestamp)        as medium,
        arg_min(campaign, event_timestamp)      as campaign,
        count(*) filter (where event_name = 'purchase') as purchases,
        coalesce(sum(event_value) filter (where event_name = 'purchase'), 0) as revenue
    from events
    group by 1, 2, 3
)

select
    session_key,
    user_pseudo_id,
    ga_session_id,
    session_date,
    session_start_at,
    session_end_at,
    event_count,
    is_engaged,
    landing_page,
    source,
    medium,
    campaign,
    purchases,
    revenue,
    md5(source || '-' || medium || '-' || campaign) as traffic_source_key
from sessions
