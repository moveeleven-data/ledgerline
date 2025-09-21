<h1 align="center">MarginSync: Subscription Usage, Billing & Margin Analytics</h1>

<p align="center">
  Accurate, auditable SaaS insights delivered from daily subscription usage with dbt and Snowflake.
  <br/><br/>
</p>

<p align="center">Designed and maintained by <a href="https://github.com/moveeleven-data">Matthew Tripodi</a></p>

---

## Key Features

| Capability | What you get |
|------------|--------------|
| **Layered modeling** | Staging → history → refined → marts, with clear separation of concerns |
| **Slowly changing dimensions** | History tables via custom `save_history` macro |
| **Default key strategy** | Self-completing dimensions guarantee referential integrity |
| **Business metrics** | Daily usage, billed amounts, overage, and margin percentage |
| **Data quality** | Tests for hash collisions, default key rules, valid dates and bounds |

---

## Architecture

### Data Flow

Usage and reference data land in staging, history retains changes, refined computes current views and billing logic, marts publish the dimensional star schema.

### Schema
  
**fact_usage** records daily subscription activity and joins to five dimensions.  
  
**fact_rate_card_daily** records daily effective unit prices by product and plan.

![MarginSync Architecture](docs/assets/erd_physical_model.png)

---

## Quickstart

**Prereqs:**  
- Snowflake account  
- dbt CLI or dbt Cloud  

### 1. Configure Snowflake

Run the setup script (or inline SQL) to create dev/prod databases, schemas, and a `dbt_executor_role` with least-privilege grants. Example:

```sql
create database if not exists margin_sync_dev;

create schema if not exists margin_sync_dev.source_data;
create schema if not exists margin_sync_dev.staging;
create schema if not exists margin_sync_dev.history;
create schema if not exists margin_sync_dev.refined;
create schema if not exists margin_sync_dev.marts;

create warehouse if not exists marginsync_wh
  with warehouse_size = xsmall auto_suspend = 60 auto_resume = true;

create role if not exists dbt_executor_role;
```

### 2. Load Sample Data

Stage a CSV of usage and load it into the landing table:

```sql
copy into source_data.subscription_usage_daily
from @usage_stage/usage_sample.csv
file_format = (type = csv, skip_header = 1);

```

The source YAML enforces freshness and a unique natural key on (customer, product, plan, report_date).

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

## Project Layout

**models/staging/**  
- Standardize sources and seeds, generate surrogate keys, add defaults.

**models/history/**  
- Incremental history via `save_history`, including synthetic closes for churned subscriptions.

**models/refined/**  
- Current views, billing and margin metrics, and an invalid rows view for QA.

**models/marts/usage/**  
- Star schema dimensions and `fact_usage`, with uniqueness and relationships tests.

**macros/**  
- Core, history, and test macros including self-completing dimensions and hash-collision checks.

**seeds/**  
- Reference CSVs for customers, products, plans, currencies, and rate cards.

**snapshots/**  
- Optional SCD2 snapshots (kept off by default).

**docs/**  
- Images, ERDs, and future BI screenshots.