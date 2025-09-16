<h1 align="center">MarketSync: dbt + Snowflake Portfolio Analytics</h1>

<p align="center">
  Transform ABC Bank’s portfolio positions into a clean star schema with history tables, macros, and tests.
  <br/><br/>
</p>

---

## Key Features

| Capability | What you get |
|------------|--------------|
| **Layered modeling** | Staging → history → refined → marts, with clear separation of concerns |
| **Slowly changing dimensions** | History tables via custom `save_history` macro (no snapshots required) |
| **Default key strategy** | Self-completing dimensions guarantee referential integrity |
| **Business metrics** | Current positions enriched with Unrealized P&L and percentage |
| **Data quality** | Custom tests for hash collisions, default key rules, and valid dates |
| **Extensibility** | Seeds for reference data, ready for external market prices in Phase 1 |

---

## Architecture

![MarketSync Architecture](docs/images/marketsync_architecture.png)

<sup>Positions and reference data land in **staging** ➜ **history** retains changes over time ➜ **refined** computes current views and P&L ➜ **marts** join into a dimensional star schema for analytics.</sup>

---

## Quickstart

**Prereqs:**  
- Snowflake account  
- dbt CLI or dbt Cloud  

### 1. Configure Snowflake

Run the setup script (or inline SQL) to create dev/prod databases, schemas, and a `dbt_executor_role` with least-privilege grants. Example:

```sql
create database if not exists market_sync_dev;
create schema if not exists market_sync_dev.source_data;
create schema if not exists market_sync_dev.staging;
create warehouse if not exists market_sync_wh with warehouse_size = xsmall auto_suspend = 60 auto_resume = true;
create role if not exists dbt_executor_role;
```

### 2. Load Sample Data

Upload a CSV of positions into a Snowflake stage and run:

```sql
copy into source_data.abc_bank_position
from @positions_stage/positions_sample.csv
file_format = (type = csv, skip_header = 1);
```

### Run dbt

Install dependencies and run a build:

```bash
dbt deps
dbt seed
dbt build
```

### 4. Explore

Open dbt docs to browse models, lineage, and freshness:

```bash
dbt docs generate && dbt docs serve
```

---

### Project Layout

## Project Layout

**models/staging/**  
— Standardize seeds and sources, generate surrogate keys, add defaults  

**models/history/**  
— Incremental history via `save_history`, including closing logic for positions  

**models/refined/**  
— Current views with Unrealized P&L  

**models/marts/portfolio/**  
— Dimensional models and fact table for star schema  

**macros/**  
— Custom macros for history, defaults, migrations, and quality tests  

**seeds/**  
— Reference CSVs (accounts, countries, currencies, exchanges, securities)  

**snapshots/**  
— Optional SCD2 snapshots (disabled by default)  

**docs/**  
— Images, ERDs, and future BI screenshots  


<p align="center">Built by <a href="https://github.com/moveeleven-data">Matthew Tripodi</a></p>
