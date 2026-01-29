# DeepBook Margin Pool Analytics on Dune

dbt models for DeepBook Margin Trading analytics on Sui, deployed to Dune Analytics.

## 🎯 Featured: Daily Pool Metrics with USD Pricing

Query the `fct_deepbook_margin_pool_daily` model to get daily snapshots of all DeepBook margin pools with USD valuations:

```sql
-- DeepBook Margin Pool Daily Metrics (USD)
SELECT
    snapshot_date,
    coin_symbol,
    total_supply_normalized,
    total_supply_usd,
    total_borrow_usd,
    utilization_rate,
    price_usd
FROM dune.sui__tmp_.fct_deepbook_margin_pool_daily
ORDER BY snapshot_date DESC, total_supply_usd DESC
```

**Sample Output (Jan 2026):**

| snapshot_date | coin_symbol | total_supply | total_supply_usd | total_borrow_usd | utilization |
|---------------|-------------|--------------|------------------|------------------|-------------|
| 2026-01-29    | USDC        | 1,950,127    | $1,950,127       | $211,382         | 10.8%       |
| 2026-01-29    | SUI         | 523,239      | $711,322         | $4,167           | 0.6%        |
| 2026-01-29    | DEEP        | 10,972,620   | $402,763         | $1,899           | 0.5%        |

## Quick Start

```bash
# 1. Setup environment
cp .env.example .env
# Edit .env with your DUNE_API_KEY and DUNE_TEAM_NAME

# 2. Install dependencies
uv sync
uv run dbt deps

# 3. Verify connection
uv run dbt debug

# 4. Build all models
uv run dbt run --select +fct_deepbook_margin_pool_daily

# 5. Run tests
uv run dbt test
```

## Models

This project builds 7 models for DeepBook margin trading analytics:

### Event Models (from `sui.events`)
| Model | Description |
|-------|-------------|
| `deepbook_margin_deposit_collateral` | Collateral deposits to margin positions |
| `deepbook_margin_loan_borrowed` | Margin loans taken from lending pools |
| `deepbook_margin_loan_repaid` | Margin loan repayments |
| `deepbook_margin_pool_asset_supplied` | Liquidity provision to margin pools |
| `deepbook_margin_pool_asset_withdrawn` | Liquidity removal from margin pools |

### Object State Model (from `sui.objects`)
| Model | Description |
|-------|-------------|
| `stg_deepbook_margin_pool_object` | Pool state snapshots (TVL, utilization, rates) |

### Daily Fact Model
| Model | Description |
|-------|-------------|
| `fct_deepbook_margin_pool_daily` | **Daily pool metrics** - TVL, volumes, utilization |

## Sample Queries

### Pool TVL Over Time (USD)
```sql
SELECT
    snapshot_date,
    coin_symbol,
    total_supply_usd as tvl_usd,
    total_borrow_usd,
    utilization_rate
FROM dune.sui__tmp_.fct_deepbook_margin_pool_daily
WHERE coin_symbol = 'USDC'
ORDER BY snapshot_date
```

### Total TVL Across All Pools
```sql
SELECT
    snapshot_date,
    sum(total_supply_usd) as total_tvl_usd,
    sum(total_borrow_usd) as total_borrowed_usd
FROM dune.sui__tmp_.fct_deepbook_margin_pool_daily
GROUP BY 1
ORDER BY 1 DESC
```

### Daily Borrow Volume by Pool (USD)
```sql
SELECT
    snapshot_date,
    coin_symbol,
    daily_borrow_volume_usd,
    daily_repay_volume_usd
FROM dune.sui__tmp_.fct_deepbook_margin_pool_daily
ORDER BY snapshot_date DESC
```

### Recent Loan Events
```sql
SELECT
    from_unixtime(timestamp_ms / 1000) as time,
    margin_pool_id,
    loan_amount / 1e6 as loan_amount_normalized
FROM dune.sui__tmp_.deepbook_margin_loan_borrowed
ORDER BY timestamp_ms DESC
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DUNE_API_KEY` | Yes | Your Dune API key from [dune.com/settings/api](https://dune.com/settings/api) |
| `DUNE_TEAM_NAME` | Yes | Your Dune team name (e.g., `sui`) |
| `DEV_SCHEMA_SUFFIX` | No | Optional suffix for dev schema isolation |

### Schema Naming

- **Dev**: `{team}__tmp_` (e.g., `sui__tmp_`)
- **Prod**: `{team}` (e.g., `sui`)

## Package Info

- **DeepBook Margin Package**: `0x97d9473771b01f77b0940c589484184b49f6444627ec121314fae6a6d36fb86b`
- **Object Type**: `margin_pool::MarginPool<T>`

## Built With

- [Dune dbt Template](https://github.com/duneanalytics/dune-dbt-template)
- [dbt-trino](https://github.com/starburstdata/dbt-trino)
