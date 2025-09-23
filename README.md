<h1 align="center">LedgerLine: Subscription Usage, Billing & Margin Analytics</h1>

<p align="center">
  Accurate, auditable SaaS insights delivered from daily subscription usage with dbt and Snowflake.
  <br/><br/>
</p>

## Business Story

LedgerLine simulates the financial heartbeat of a modern B2B SaaS company.

Customers subscribe to products, get a bundle of included usage, and pay overage once they cross that threshold.  

The core services are:  
- **PROD-API** - Every API call counts toward your bill
- **PROD-ETL** - Rows processed in the pipeline
- **PROD-ALRT** - Alerts or notifications sent

Pricing isn’t fixed. A **daily price book** sets the official unit rate for each product and plan, making room for promos or mid-month changes.  

Every night, the metering system emits a **usage feed**. Picture rows like:  
*Customer X made 12,000 API calls on 2025-09-16.*  
*Customer Y processed 250,000 ETL rows on the same day.*  

Seed files simulate system-of-record extracts: CRM customers, catalog products and plans, plus countries and currencies.

From there, we transform this foundation into **marts**. At the center is **fact_usage**, one row per customer, product, plan, and day.

This star schema is what makes the business questions answerable:  
- **Finance** can track recurring revenue, ARR/MRR, and the split between subscriptions and overages.  
- **Product** can measure which services drive growth and how pricing changes ripple through usage.  
- **Customer Success** can spot accounts pushing limits (prime upsell targets) or sliding into churn.  
- **Executives** can see growth across geographies and the contribution of new offerings like Alerts.  

LedgerLine shows how raw usage logs and simple reference datasets can become an auditable subscription billing engine that powers analytics across the company.  

---

## Architecture

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