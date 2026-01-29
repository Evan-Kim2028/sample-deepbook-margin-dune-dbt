{{
  config(
    materialized='incremental',
    unique_key=['margin_pool_id', 'snapshot_date'],
    incremental_strategy='merge',
    tags=['deepbook_margin', 'daily']
  )
}}

{#
  DeepBook Margin Pool Daily Snapshots

  End-of-day pool metrics with USD valuations for easy consumption.

  **Package**: `0x97d9473771b01f77b0940c589484184b49f6444627ec121314fae6a6d36fb86b`

  Grain: One row per margin_pool_id per snapshot_date

  Key Metrics:
  - TVL (total_supply) and borrowing (total_borrow) in native tokens and USD
  - Utilization rate
  - Daily volumes (supply, withdraw, borrow, repay) in native tokens and USD
  - Active positions count
#}

-- Supply volume from events
with supply_volume as (
    select
        date(from_unixtime(timestamp_ms / 1000)) as snapshot_date,
        margin_pool_id,
        sum(supply_amount) as supply_volume
    from {{ ref('deepbook_margin_pool_asset_supplied') }}
    {% if is_incremental() %}
        where date(from_unixtime(timestamp_ms / 1000)) >= (select date_add('day', -7, max(snapshot_date)) from {{ this }})
    {% endif %}
    group by 1, 2
),

-- Withdraw volume from events
withdraw_volume as (
    select
        date(from_unixtime(timestamp_ms / 1000)) as snapshot_date,
        margin_pool_id,
        sum(withdraw_amount) as withdraw_volume
    from {{ ref('deepbook_margin_pool_asset_withdrawn') }}
    {% if is_incremental() %}
        where date(from_unixtime(timestamp_ms / 1000)) >= (select date_add('day', -7, max(snapshot_date)) from {{ this }})
    {% endif %}
    group by 1, 2
),

-- Borrow volume from events
borrow_volume as (
    select
        date(from_unixtime(timestamp_ms / 1000)) as snapshot_date,
        margin_pool_id,
        sum(loan_amount) as borrow_volume
    from {{ ref('deepbook_margin_loan_borrowed') }}
    {% if is_incremental() %}
        where date(from_unixtime(timestamp_ms / 1000)) >= (select date_add('day', -7, max(snapshot_date)) from {{ this }})
    {% endif %}
    group by 1, 2
),

-- Repay volume from events
repay_volume as (
    select
        date(from_unixtime(timestamp_ms / 1000)) as snapshot_date,
        margin_pool_id,
        sum(repay_amount) as repay_volume
    from {{ ref('deepbook_margin_loan_repaid') }}
    {% if is_incremental() %}
        where date(from_unixtime(timestamp_ms / 1000)) >= (select date_add('day', -7, max(snapshot_date)) from {{ this }})
    {% endif %}
    group by 1, 2
),

daily_volume_agg as (
    select
        coalesce(s.snapshot_date, w.snapshot_date, b.snapshot_date, r.snapshot_date) as snapshot_date,
        coalesce(s.margin_pool_id, w.margin_pool_id, b.margin_pool_id, r.margin_pool_id) as margin_pool_id,
        coalesce(s.supply_volume, 0) as supply_volume,
        coalesce(w.withdraw_volume, 0) as withdraw_volume,
        coalesce(b.borrow_volume, 0) as borrow_volume,
        coalesce(r.repay_volume, 0) as repay_volume
    from supply_volume s
    full outer join withdraw_volume w on s.snapshot_date = w.snapshot_date and s.margin_pool_id = w.margin_pool_id
    full outer join borrow_volume b on coalesce(s.snapshot_date, w.snapshot_date) = b.snapshot_date and coalesce(s.margin_pool_id, w.margin_pool_id) = b.margin_pool_id
    full outer join repay_volume r on coalesce(s.snapshot_date, w.snapshot_date, b.snapshot_date) = r.snapshot_date and coalesce(s.margin_pool_id, w.margin_pool_id, b.margin_pool_id) = r.margin_pool_id
),

pool_objects_ranked as (
    select
        snapshot_date,
        margin_pool_id,
        asset_type,
        coin_symbol,
        coin_decimals,
        total_borrow_normalized,
        total_supply_normalized,
        utilization_rate,
        available_liquidity_normalized,
        active_positions_count,
        timestamp_ms,
        version as last_object_version,
        row_number() over (
            partition by margin_pool_id, snapshot_date
            order by timestamp_ms desc, version desc
        ) as rn
    from {{ ref('stg_deepbook_margin_pool_object') }}
    where 1=1
        {% if is_incremental() %}
            and snapshot_date >= (select date_add('day', -7, max(snapshot_date)) from {{ this }})
        {% endif %}
),

pool_daily_base as (
    select * from pool_objects_ranked where rn = 1
),

-- Daily prices from Dune's prices.day table
daily_prices as (
    select price_date, symbol, price_usd
    from (
        select
            date(timestamp) as price_date,
            symbol,
            price as price_usd,
            row_number() over (partition by date(timestamp), symbol order by timestamp desc) as rn
        from prices.day
        where blockchain = 'sui'
          and symbol in ('SUI', 'USDC', 'DEEP')
    ) ranked
    where rn = 1
)

select
    p.snapshot_date,
    p.margin_pool_id,
    p.coin_symbol,

    -- === POOL STATE (normalized) ===
    p.total_supply_normalized,
    p.total_borrow_normalized,
    p.available_liquidity_normalized,
    p.utilization_rate,
    p.active_positions_count,

    -- === POOL STATE (USD) ===
    case
        when upper(p.coin_symbol) in ('USDC', 'USDT', 'AUSD') then 1.0
        else coalesce(pr.price_usd, 0)
    end as price_usd,
    p.total_supply_normalized * case
        when upper(p.coin_symbol) in ('USDC', 'USDT', 'AUSD') then 1.0
        else coalesce(pr.price_usd, 0)
    end as total_supply_usd,
    p.total_borrow_normalized * case
        when upper(p.coin_symbol) in ('USDC', 'USDT', 'AUSD') then 1.0
        else coalesce(pr.price_usd, 0)
    end as total_borrow_usd,

    -- === DAILY VOLUMES (normalized) ===
    coalesce(v.supply_volume, 0) / power(10, coalesce(p.coin_decimals, 9)) as daily_supply_volume,
    coalesce(v.withdraw_volume, 0) / power(10, coalesce(p.coin_decimals, 9)) as daily_withdraw_volume,
    coalesce(v.borrow_volume, 0) / power(10, coalesce(p.coin_decimals, 9)) as daily_borrow_volume,
    coalesce(v.repay_volume, 0) / power(10, coalesce(p.coin_decimals, 9)) as daily_repay_volume,

    -- === DAILY VOLUMES (USD) ===
    (coalesce(v.supply_volume, 0) / power(10, coalesce(p.coin_decimals, 9))) * case
        when upper(p.coin_symbol) in ('USDC', 'USDT', 'AUSD') then 1.0
        else coalesce(pr.price_usd, 0)
    end as daily_supply_volume_usd,
    (coalesce(v.withdraw_volume, 0) / power(10, coalesce(p.coin_decimals, 9))) * case
        when upper(p.coin_symbol) in ('USDC', 'USDT', 'AUSD') then 1.0
        else coalesce(pr.price_usd, 0)
    end as daily_withdraw_volume_usd,
    (coalesce(v.borrow_volume, 0) / power(10, coalesce(p.coin_decimals, 9))) * case
        when upper(p.coin_symbol) in ('USDC', 'USDT', 'AUSD') then 1.0
        else coalesce(pr.price_usd, 0)
    end as daily_borrow_volume_usd,
    (coalesce(v.repay_volume, 0) / power(10, coalesce(p.coin_decimals, 9))) * case
        when upper(p.coin_symbol) in ('USDC', 'USDT', 'AUSD') then 1.0
        else coalesce(pr.price_usd, 0)
    end as daily_repay_volume_usd,

    -- === DAY-OVER-DAY CHANGES ===
    p.total_supply_normalized - lag(p.total_supply_normalized) over (
        partition by p.margin_pool_id order by p.snapshot_date
    ) as daily_supply_change,
    p.total_borrow_normalized - lag(p.total_borrow_normalized) over (
        partition by p.margin_pool_id order by p.snapshot_date
    ) as daily_borrow_change,
    p.utilization_rate - lag(p.utilization_rate) over (
        partition by p.margin_pool_id order by p.snapshot_date
    ) as daily_utilization_change,

    -- === METADATA ===
    p.asset_type,
    now() as updated_at

from pool_daily_base p
left join daily_volume_agg v
    on v.snapshot_date = p.snapshot_date
    and v.margin_pool_id = p.margin_pool_id
left join daily_prices pr
    on pr.price_date = p.snapshot_date
    and upper(pr.symbol) = upper(p.coin_symbol)

order by p.snapshot_date desc, p.total_supply_normalized desc nulls last
