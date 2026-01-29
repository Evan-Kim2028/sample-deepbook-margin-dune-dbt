{{
  config(
    materialized='incremental',
    unique_key=['object_id', 'version'],
    incremental_strategy='merge',
    tags=['deepbook_margin']
  )
}}

{#
  DeepBook Margin Pool Object Staging

  Extracts margin pool object state from sui.objects and joins with coin metadata.

  **Package**: `0x97d9473771b01f77b0940c589484184b49f6444627ec121314fae6a6d36fb86b`

  **Object Type**: `margin_pool::MarginPool<T>`

  Grain: One row per object mutation (version change)
#}

with coin_metadata as (
    -- Hardcoded coin metadata for common Sui coins
    select '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI' as coin_type, 'SUI' as coin_symbol, 9 as coin_decimals, 'Sui' as coin_name
    union all
    select '0x2::sui::SUI', 'SUI', 9, 'Sui'
    union all
    select '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC', 'USDC', 6, 'USD Coin'
    union all
    select '0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN', 'wUSDC', 6, 'Wormhole USDC'
    union all
    select '0xdeeb7a4662eec9f2f3def03fb937a663dddaa2e215b8078a284d026b7946c270::deep::DEEP', 'DEEP', 6, 'DeepBook Token'
),

margin_pool_objects as (
    select
        cast(object_id as varchar) as object_id,
        version,
        type_ as type,
        object_status,
        object_json,
        timestamp_ms,

        -- Extract asset type from generic type parameter
        regexp_extract(type_, '<(.+)>$', 1) as asset_type,

        -- Primary identifiers
        json_extract_scalar(object_json, '$.id.id') as margin_pool_id,

        -- Pool state (core lending metrics)
        try_cast(json_extract_scalar(object_json, '$.state.total_borrow') as double) as total_borrow,
        try_cast(json_extract_scalar(object_json, '$.state.total_supply') as double) as total_supply,
        try_cast(json_extract_scalar(object_json, '$.state.borrow_shares') as double) as borrow_shares,
        try_cast(json_extract_scalar(object_json, '$.state.supply_shares') as double) as supply_shares,
        try_cast(json_extract_scalar(object_json, '$.state.last_update_timestamp') as bigint) as last_update_timestamp_ms,

        -- Vault balance (available liquidity)
        try_cast(json_extract_scalar(object_json, '$.vault') as double) as vault_balance,

        -- Protocol fees
        try_cast(json_extract_scalar(object_json, '$.protocol_fees.fees_per_share') as double) as fees_per_share,
        try_cast(json_extract_scalar(object_json, '$.protocol_fees.maintainer_fees') as double) as maintainer_fees,
        try_cast(json_extract_scalar(object_json, '$.protocol_fees.protocol_fees') as double) as protocol_fees,
        try_cast(json_extract_scalar(object_json, '$.protocol_fees.total_shares') as double) as protocol_fee_total_shares,
        try_cast(json_extract_scalar(object_json, '$.protocol_fees.referrals.size') as bigint) as referrals_count,

        -- Position tracking
        try_cast(json_extract_scalar(object_json, '$.positions.positions.size') as bigint) as active_positions_count,
        json_extract_scalar(object_json, '$.positions.positions.id.id') as positions_table_id,

        -- Interest rate configuration
        try_cast(json_extract_scalar(object_json, '$.config.interest_config.base_rate') as double) as interest_base_rate,
        try_cast(json_extract_scalar(object_json, '$.config.interest_config.base_slope') as double) as interest_base_slope,
        try_cast(json_extract_scalar(object_json, '$.config.interest_config.excess_slope') as double) as interest_excess_slope,
        try_cast(json_extract_scalar(object_json, '$.config.interest_config.optimal_utilization') as double) as interest_optimal_utilization,

        -- Pool configuration (risk parameters)
        try_cast(json_extract_scalar(object_json, '$.config.margin_pool_config.max_utilization_rate') as double) as max_utilization_rate,
        try_cast(json_extract_scalar(object_json, '$.config.margin_pool_config.min_borrow') as double) as min_borrow,
        try_cast(json_extract_scalar(object_json, '$.config.margin_pool_config.protocol_spread') as double) as protocol_spread,
        try_cast(json_extract_scalar(object_json, '$.config.margin_pool_config.supply_cap') as double) as supply_cap,
        try_cast(json_extract_scalar(object_json, '$.config.margin_pool_config.rate_limit_enabled') as boolean) as rate_limit_enabled,
        try_cast(json_extract_scalar(object_json, '$.config.margin_pool_config.rate_limit_capacity') as double) as rate_limit_capacity,

        -- Rate limiter state
        try_cast(json_extract_scalar(object_json, '$.rate_limiter.available') as double) as rate_limiter_available,
        try_cast(json_extract_scalar(object_json, '$.rate_limiter.capacity') as double) as rate_limiter_capacity,
        try_cast(json_extract_scalar(object_json, '$.rate_limiter.enabled') as boolean) as rate_limiter_enabled,
        try_cast(json_extract_scalar(object_json, '$.rate_limiter.last_updated_ms') as bigint) as rate_limiter_last_updated_ms,

        -- Allowed DeepBook pools (for margin trading)
        json_format(json_extract(object_json, '$.allowed_deepbook_pools.contents')) as allowed_deepbook_pools_json

    from {{ source('sui', 'objects') }}
    where type_ like '0x97d9473771b01f77b0940c589484184b49f6444627ec121314fae6a6d36fb86b::margin_pool::MarginPool<%'
    {% if is_incremental() %}
        and timestamp_ms >= (select coalesce(max(timestamp_ms), 0) from {{ this }})
    {% else %}
        -- Initial backfill: limit to last 30 days to prevent timeout
        and timestamp_ms >= cast(to_unixtime(now() - interval '30' day) * 1000 as bigint)
    {% endif %}
)

select
    m.timestamp_ms,
    from_unixtime(m.timestamp_ms / 1000) as block_timestamp,
    date(from_unixtime(m.timestamp_ms / 1000)) as snapshot_date,
    m.object_id,
    m.version,
    m.type,
    m.object_status,

    -- Identifiers
    m.margin_pool_id,
    m.asset_type,

    -- Coin metadata
    coalesce(cm.coin_symbol, 'UNKNOWN') as coin_symbol,
    coalesce(cm.coin_decimals, 9) as coin_decimals,
    coalesce(cm.coin_name, 'Unknown') as coin_name,

    -- Pool state (raw units)
    m.total_borrow,
    m.total_supply,
    m.borrow_shares,
    m.supply_shares,
    m.last_update_timestamp_ms,
    from_unixtime(m.last_update_timestamp_ms / 1000) as last_update_timestamp,
    m.vault_balance,

    -- Pool state (normalized - human-readable)
    case
        when cm.coin_decimals is not null then m.total_borrow / power(10, cm.coin_decimals)
        else null
    end as total_borrow_normalized,
    case
        when cm.coin_decimals is not null then m.total_supply / power(10, cm.coin_decimals)
        else null
    end as total_supply_normalized,
    case
        when cm.coin_decimals is not null then m.vault_balance / power(10, cm.coin_decimals)
        else null
    end as vault_balance_normalized,

    -- Derived metrics
    case
        when m.supply_shares > 0 then m.total_supply / m.supply_shares
        else null
    end as supply_share_price,
    case
        when m.borrow_shares > 0 then m.total_borrow / m.borrow_shares
        else null
    end as borrow_share_price,
    case
        when m.total_supply > 0 then m.total_borrow / m.total_supply
        else 0
    end as utilization_rate,
    m.total_supply - m.total_borrow as available_liquidity,
    case
        when cm.coin_decimals is not null then (m.total_supply - m.total_borrow) / power(10, cm.coin_decimals)
        else null
    end as available_liquidity_normalized,

    -- Protocol fees
    m.fees_per_share,
    m.maintainer_fees,
    m.protocol_fees,
    m.protocol_fee_total_shares,
    m.referrals_count,

    -- Position tracking
    m.active_positions_count,
    m.positions_table_id,

    -- Interest configuration
    m.interest_base_rate,
    m.interest_base_slope,
    m.interest_excess_slope,
    m.interest_optimal_utilization,

    -- Pool configuration
    m.max_utilization_rate,
    m.min_borrow,
    m.protocol_spread,
    m.supply_cap,
    m.rate_limit_enabled,
    m.rate_limit_capacity,

    -- Rate limiter state
    m.rate_limiter_available,
    m.rate_limiter_capacity,
    m.rate_limiter_enabled,
    m.rate_limiter_last_updated_ms,
    from_unixtime(m.rate_limiter_last_updated_ms / 1000) as rate_limiter_last_updated_timestamp,

    -- Allowed pools
    m.allowed_deepbook_pools_json,

    -- Metadata
    now() as updated_at

from margin_pool_objects m
left join coin_metadata cm
    on case
        -- Handle SUI short form
        when m.asset_type = '0x2::sui::SUI'
        then '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI'
        else m.asset_type
    end = cm.coin_type
