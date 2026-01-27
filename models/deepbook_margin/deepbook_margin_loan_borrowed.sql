{{
  config(
    materialized='incremental',
    unique_key=['transaction_digest', 'event_index'],
    tags=['deepbook'],
    incremental_strategy='merge'
  )
}}

with raw_events as (
    select
        transaction_digest,
        event_index,
        timestamp_ms,
        sender,
        event_type,
        event_json
    from {{ source('sui', 'events') }}
    where event_type = '0x97d9473771b01f77b0940c589484184b49f6444627ec121314fae6a6d36fb86b::margin_manager::LoanBorrowedEvent'
    {% if is_incremental() %}
      and timestamp_ms >= (select coalesce(max(timestamp_ms), 0) from {{ this }})
    {% endif %}
)

select
    transaction_digest,
    event_index,
    timestamp_ms,
    sender,
    event_type,
    try_cast(json_extract_scalar(event_json, '$.loan_amount') as double) as loan_amount,
    try_cast(json_extract_scalar(event_json, '$.loan_shares') as double) as loan_shares,
    json_extract_scalar(event_json, '$.margin_manager_id') as margin_manager_id,
    json_extract_scalar(event_json, '$.margin_pool_id') as margin_pool_id,
    try_cast(json_extract_scalar(event_json, '$.timestamp') as bigint) as event_timestamp,
    now() as updated_at
from raw_events
