# Development Guide & Gotchas

Tips and gotchas for developing dbt models on Dune.

## Environment Setup

### Required Environment Variables
```bash
export DUNE_API_KEY="your_api_key"
export DUNE_TEAM_NAME="sui"  # Your Dune team name
export DEV_SCHEMA_SUFFIX=""  # Optional: for PR isolation
```

Or use a `.env` file:
```bash
cp .env.example .env
# Edit .env with your values
export $(cat .env | xargs)
```

## Dune/Trino SQL Gotchas

### 1. JSON Handling

**Use `json_extract_scalar` for simple values:**
```sql
-- ✅ Correct
json_extract_scalar(object_json, '$.field') as field_value

-- ❌ Wrong (returns json type, not string)
json_extract(object_json, '$.field') as field_value
```

**Use `json_format` for JSON arrays/objects:**
```sql
-- ✅ Correct - converts JSON to string
json_format(json_extract(object_json, '$.array_field')) as array_string

-- ❌ Wrong - "Unsupported type: json" error
json_extract(object_json, '$.array_field') as array_json

-- ❌ Wrong - "Cannot cast '[...]' to varchar" error
cast(json_extract(object_json, '$.array_field') as varchar)
```

### 2. Time-Bounded Backfills (Critical!)

Always limit initial backfills to prevent timeouts on large tables:

```sql
{% if is_incremental() %}
    and timestamp_ms >= (select coalesce(max(timestamp_ms), 0) from {{ this }})
{% else %}
    -- Initial backfill: limit to last 30 days to prevent timeout
    and timestamp_ms >= cast(to_unixtime(now() - interval '30' day) * 1000 as bigint)
{% endif %}
```

Without this, queries against `sui.events` or `sui.objects` will timeout (30s default).

### 3. Boolean Casting

```sql
-- ✅ Correct
try_cast(json_extract_scalar(obj, '$.enabled') as boolean)

-- Works for: "true", "false", true, false
```

### 4. Timestamp Conversion

```sql
-- Milliseconds to timestamp
from_unixtime(timestamp_ms / 1000) as block_timestamp

-- Timestamp to date
date(from_unixtime(timestamp_ms / 1000)) as snapshot_date

-- Current time to milliseconds (for filters)
cast(to_unixtime(now() - interval '30' day) * 1000 as bigint)
```

### 5. QUALIFY Clause (Not Supported)

Dune's Trino doesn't support `QUALIFY`. Use subquery with `WHERE` instead:

```sql
-- ❌ Wrong - "mismatched input 'qualify'" error
SELECT * FROM table
QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1

-- ✅ Correct - use subquery
SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY id ORDER BY ts DESC) as rn
    FROM table
) ranked
WHERE rn = 1
```

### 6. LIMIT Clause

In `dbt show --inline`, use `--limit` flag instead of SQL LIMIT:
```bash
# ✅ Correct
uv run dbt show --limit 10 --inline "SELECT * FROM table ORDER BY date DESC"

# ❌ Wrong - "mismatched input 'limit'" error
uv run dbt show --inline "SELECT * FROM table LIMIT 10"
```

## Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `Read timed out (30.0)` | Query too large / no time bounds | Add backfill limits (see above) |
| `Unsupported type: json` | Using `json_extract` for output column | Use `json_format()` wrapper |
| `Cannot cast '[...]' to varchar` | Casting JSON array to varchar | Use `json_format()` instead |
| `access denied` | Wrong team name | Check `DUNE_TEAM_NAME` matches your Dune account |
| `Table not found` | Model not built yet | Run `uv run dbt run --select model_name` |

## Incremental Model Patterns

### Merge Strategy (for upserts)
```sql
{{
  config(
    materialized='incremental',
    unique_key=['id_col1', 'id_col2'],
    incremental_strategy='merge'
  )
}}
```

### Append Strategy (for event logs)
```sql
{{
  config(
    materialized='incremental',
    incremental_strategy='append'
  )
}}
```

## Testing Locally

```bash
# Compile SQL without running (fast validation)
uv run dbt compile --select model_name

# Preview data
uv run dbt show --limit 5 --inline "SELECT * FROM dune.sui__tmp_.model_name"

# Run single model
uv run dbt run --select model_name

# Run model + all upstream dependencies
uv run dbt run --select +model_name

# Full refresh (rebuild from scratch)
uv run dbt run --select model_name --full-refresh
```

## Schema Naming

| Target | Schema Result |
|--------|---------------|
| `dev` (default) | `{team}__tmp_` |
| `dev` + `DEV_SCHEMA_SUFFIX=pr123` | `{team}__tmp_pr123` |
| `prod` | `{team}` |

## Querying in Dune UI

Always include the `dune` catalog prefix:
```sql
-- ✅ Correct
SELECT * FROM dune.sui__tmp_.fct_deepbook_margin_pool_daily

-- ❌ Wrong (won't work in Dune UI)
SELECT * FROM sui__tmp_.fct_deepbook_margin_pool_daily
```
