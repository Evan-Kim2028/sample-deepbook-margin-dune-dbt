# sample-deepbook-margin-dune-dbt

Minimal dbt project that builds two DeepBook margin models on Dune.

Built from Dune's dbt template: https://github.com/duneanalytics/dune-dbt-template

## Setup

1. Create an env file:

```bash
cp .env.example .env
```

2. Fill in:

- `DUNE_API_KEY`
- `DUNE_TEAM_NAME` (your Dune team name; used for the base schema)
- `DEV_SCHEMA_SUFFIX` (optional)

3. Install deps + run:

```bash
uv sync
uv run dbt deps
uv run dbt debug
uv run dbt run --select deepbook_margin_deposit_collateral deepbook_margin_loan_borrowed
uv run dbt test --select deepbook_margin_deposit_collateral deepbook_margin_loan_borrowed
```

## What gets built

These models materialize to your dev schema on Dune:

- `dune.sui__tmp_.deepbook_margin_deposit_collateral`
- `dune.sui__tmp_.deepbook_margin_loan_borrowed`

## Quickstart

```bash
# 1) install
uv sync
uv run dbt deps

# 2) sanity check connection
uv run dbt debug

# 3) build models
uv run dbt run --select deepbook_margin_deposit_collateral deepbook_margin_loan_borrowed

# 4) run schema tests
uv run dbt test --select deepbook_margin_deposit_collateral deepbook_margin_loan_borrowed
```

## Sample queries

In Dune SQL editor (Trino):

```sql
-- Deposit collateral events (latest 100)
select *
from dune.sui__tmp_.deepbook_margin_deposit_collateral
order by timestamp_ms desc
limit 100;

-- Loan borrowed events (latest 100)
select *
from dune.sui__tmp_.deepbook_margin_loan_borrowed
order by timestamp_ms desc
limit 100;

-- Daily totals (deposit collateral)
select
  date(from_unixtime(timestamp_ms / 1000)) as day,
  count(*) as events,
  sum(amount) as total_amount
from dune.sui__tmp_.deepbook_margin_deposit_collateral
group by 1
order by 1 desc;
```

## FAQ

### Where do I put my Dune API key?

Put it in `.env` (this repo ships `.env.example` and ignores `.env` via `.gitignore`).

### What is `DUNE_TEAM_NAME`?

It controls the base schema used for dbt runs on Dune (for this sample, the models are written into `sui__tmp_`).

### My query returns "Table not found"

Run `uv run dbt run --select deepbook_margin_deposit_collateral deepbook_margin_loan_borrowed` first, then re-run the query.
