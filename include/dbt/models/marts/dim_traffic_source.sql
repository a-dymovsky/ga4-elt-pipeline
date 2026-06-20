-- Traffic-source dimension: one row per distinct source / medium / campaign,
-- with a derived channel_group (the kind of rollup an analyst actually queries).

with distinct_sources as (
    select distinct source, medium, campaign
    from {{ ref('stg_events') }}
)

select
    md5(source || '-' || medium || '-' || campaign) as traffic_source_key,
    source,
    medium,
    campaign,
    case
        when medium in ('cpc', 'ppc', 'paid') then 'paid search'
        when medium = 'organic'               then 'organic search'
        when medium = 'email'                 then 'email'
        when medium = 'social'                then 'social'
        when source = '(direct)'              then 'direct'
        else 'other'
    end as channel_group
from distinct_sources
