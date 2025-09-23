<h1 align="center">LedgerLine: Subscription Usage, Billing & Margin Analytics</h1>

<p align="center">
  Accurate, auditable SaaS insights delivered from daily subscription usage with dbt and Snowflake.
  <br/><br/>
</p>

---

## Business Story

LedgerLine models the financial pulse of a SaaS platform.  

Customers subscribe to products, choose plans with included units, and are billed from a price book that sets daily rates. A usage feed records what each customer consumed.

Seeds provide the system of record (customers, products, plans, prices). dbt turns this raw activity into staging, history, refined, and marts, ending in a star schema with `fact_usage` at the center.

From here, you can answer:  

*What revenue came from overages? Which products drive growth? Who is ready for an upsell? How do ARR/MRR trends look across geographies?*

---

## Architecture

### Data Flow

Usage and reference data land in staging, history retains changes, refined computes billing logic, marts publish the dimensional star schema.

### Schema
  
**fact_usage** records daily subscription activity and joins to five dimensions.  
  
**fact_price_book_daily** records daily effective unit prices by product and plan.

![LedgerLine Architecture](docs/assets/erd_phys_model.png)

---

## Quickstart

**Prereqs:**  
- Snowflake account  
- dbt CLI or dbt Cloud  

### 1. Configure Snowflake

Run the setup script (or inline SQL) to create dev/prod databases, schemas, and a `dbt_executor_role` with least-privilege grants. Example:

```sql
create database if not exists ledgerline_dev;
create database if not exists ledgerline_prod;

create schema if not exists ledgerline_dev.source_data;
create schema if not exists ledgerline_dev.staging;
create schema if not exists ledgerline_dev.history;
create schema if not exists ledgerline_dev.refined;
create schema if not exists ledgerline_dev.marts;

create warehouse if not exists ledgerline_wh
  with warehouse_size = xsmall auto_suspend = 60 auto_resume = true;

create role if not exists dbt_executor_role;
```

### 2. Load Sample Data

Put CSVs in the repo under ./seeds: customers.csv, products.csv, plans.csv, currencies.csv, countries.csv, 
price_book_daily.csv, usage_daily.csv

```sql
dbt seed
```

The source YAML enforces freshness and a unique natural key on (customer, product, plan, report_date).

### 3. Run dbt

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
- Reference CSVs for customers, products, plans, currencies, and price books.

**docs/**  
- Images, ERDs, and future BI screenshots.


<p align="center">Designed and maintained by <a href="https://github.com/moveeleven-data">Matthew Tripodi</a></p>