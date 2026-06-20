-- Staging: one tidy row per event. The real GA4 modeling work is here --
-- prying the useful fields out of the JSON event_params payload and giving
-- everything sane names and types so the marts stay readable.

with source as (
    select * from {{ source('raw', 'events') }}
),

cleaned as (
    select
        event_date,
        event_timestamp,
        to_timestamp(event_timestamp / 1000000) as event_at,
        event_name,
        user_pseudo_id,
        ga_session_id,
        session_engaged,
        device_category,
        operating_system,
        browser,
        country,
        coalesce(nullif(traffic_source_source, ''), '(direct)') as source,
        coalesce(nullif(traffic_source_medium, ''), '(none)')   as medium,
        coalesce(nullif(traffic_source_campaign, ''), '(not set)') as campaign,
        json_extract_string(event_params, '$.page_location') as page_location,
        json_extract_string(event_params, '$.page_title')    as page_title,
        try_cast(json_extract_string(event_params, '$.value') as double) as event_value,
        json_extract_string(event_params, '$.currency') as currency
    from source
)

select * from cleaned
