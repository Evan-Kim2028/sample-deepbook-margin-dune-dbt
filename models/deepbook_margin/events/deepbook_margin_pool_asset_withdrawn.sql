{{
  config(
    materialized='incremental',
    unique_key=['transaction_digest', 'event_index'],
    tags=['deepbook'],
    incremental_strategy='merge'
  )
}}

{# Event: 0x97d9473771b01f77b0940c589484184b49f6444627ec121314fae6a6d36fb86b::margin_pool::AssetWithdrawn #}

with raw_events as (
    select
        transaction_digest,
        event_index,
        timestamp_ms,
        sender,
        event_type,
        event_json
    from {{ source('sui', 'events') }}
    where event_type = '0x97d9473771b01f77b0940c589484184b49f6444627ec121314fae6a6d36fb86b::margin_pool::AssetWithdrawn'
    {% if is_incremental() %}
      and timestamp_ms >= (select coalesce(max(timestamp_ms), 0) from {{ this }})
    {% else %}
      -- Initial backfill: limit to last 30 days to prevent timeout
      and timestamp_ms >= cast(to_unixtime(now() - interval '30' day) * 1000 as bigint)
    {% endif %}
)

select
    transaction_digest,
    event_index,
    timestamp_ms,
    sender,
    event_type,
    json_extract_scalar(event_json, '$.margin_pool_id') as margin_pool_id,
    json_extract_scalar(event_json, '$.supplier_cap_id') as supplier_cap_id,
    json_extract_scalar(event_json, '$.asset_type.name') as asset_type,
    try_cast(json_extract_scalar(event_json, '$.withdraw_amount') as double) as withdraw_amount,
    try_cast(json_extract_scalar(event_json, '$.withdraw_shares') as double) as withdraw_shares,
    try_cast(json_extract_scalar(event_json, '$.timestamp') as bigint) as event_timestamp,
    now() as updated_at
from raw_events
