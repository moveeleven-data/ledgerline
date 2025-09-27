## LedgerLine Changelog

### 2025-09-27

- Completed documentation across entire repo.
- Set up schema and connection settings for the 'build-all' QA and PROD jobs.
- Verified the QA job runs, produces accurate data in the correct database and schema.

### 2025-09-25

- Completed documentation of all macros and analyses.
- Implemented self-completing dimensions in all dims except currency.
- Simplified fact_usage mart by creating an intermediate table to calculating pricing.

### 2025-09-22

Rebuild all layers to accomodate LederLine dataset. Verify all models are build and clean data lands in Snowflake.

### 2025-09-21 (Port to LedgerLine)

- Rebuilt the pipeline so that STG and HIST now use exchange and currency in the business key
- Tightened HIST_ABC_BANK_POSITION so that only valid open positions are written, leaving HISTORY clean.
- Forked MarketSync scaffold into new repo `ledgerline`.
- Renamed project in `dbt_project.yml`, README, and YAML configs.
- Reset seeds to new SaaS domain (customers, products, plans, usage, price_book).
- Rewired sources to point at new SaaS raw tables.
- Confirmed project compiles with `dbt parse`.

### 2025-09-19

- Ref models now only keep the latest *open* positions, cleaning up confusing edge cases.  
- Dimensions standardized with default rows to keep fact joins consistent.
- Added lightweight QA checks (row counts, missing keys, metric sanity).
- Tagged and organized analyses for easier daily diagnostics.  
- Introduced helper macros for dev fixtures and guardrail tests.

The trickiest challenge was defining “latest” correctly. First approach was to take the latest OPEN row from the history table. But that let CLOSED events sneak in. Imagine a position that’s OPEN on Sept 1, then CLOSED on Sept 5. If you “pick the latest OPEN,” you’ll grab Sept 1 even though the true latest event is the Sept 5 CLOSE. That made our ref model claim the position was still open, produced “invalid” leaks.

We fixed it by flipping the logic: first compute latest_any per position key (order by report_date desc, load_ts_utc desc), then keep it only if it’s OPEN. In other words, “what’s the single newest event?” and only if that event is OPEN does it appear in the ref. We also added a separate __invalid view for null/zero edge cases plus a singular test that fails if any such rows leak into the ref.

Big picture, the hardest part of dbt isn't the SQL but agreeing on “what’s the truth” and enforcing it consistently.


### 2025-09-18

- Fixed HIST position model to close positions that are not present in current batch.

### 2025-09-17

- Reorganized macros into folders.
- Refactored SQL across all layers.
- Defined a singular test to check Positions in STG are unique per day.
- Improve hashing strategy for abc_bank_position_diff_fields macro.

### 2025-09-16

- Reorganized the history layer by moving all HIST_ models into a dedicated folder. Built the full flow from staging through history into refined, verifying incremental logic, surrogate keys, and synthetic close handling.
- Cleaned dimension models by removing duplicates, dropping placeholders, and fixing the dim_security reference. Standardized naming, added row_type, and aligned seeds with account and position codes.
- Validated refined against history with solid alignment and no mismatches. Confirmed mart dimensions pass uniqueness and not-null tests.

### 2025-09-15 (Phase 1 Complete)

- Restructured the repo: moved sources into `models/sources/`, created `marts/portfolio/`.
- Added `docs/` folder and drafted the first README.
- Updated `dbt_project.yml` with schema-by-tier defaults and `persist_docs`.
- Provisioned Snowflake dev and prod environments with executor role and least-privilege grants.
- Created landing table in `source_data`, loaded a sample CSV via `COPY INTO`.
- Configured source freshness checks.
- Confirmed `dbt build` runs green with seeds and sources validated.

---

### 2025-09-14 (First Baseline)

## High-Level

This is the initial version of the project I built while teaching myself the fundamentals of dbt. It models a fictional ABC Bank’s positions and reference data in Snowflake. The architecture already follows the core dbt layering pattern (staging → history → refined → marts), with seeds for static entities, macros for SCD handling and quality checks, and a simple star schema for portfolio analytics. At this point the foundation is in place, but documentation, README polish, and BI integration are still to come.

---

## Root
- **README.md**: header only.  
- **.gitignore**: ignores dbt artifacts, logs, secrets, sample CSV.  
- **dbt_project.yml**: config for paths, groups, seeds with post-hook for load timestamps.  
- **packages.yml / package-lock.yml**: pins `dbt_utils` 1.1.1.  
- **analyses/**: empty placeholder.  
- **notebooks/**: one empty notebook.  
- **.idea/**: IDE metadata.  

---

## Seeds
- CSVs for accounts, countries, currencies, exchanges, securities.  
- `seeds.yml` applies not-null and unique tests.  
- Used for stable reference data.  

---

## Snapshots
- Templates for SCD2 snapshots across all entities.  
- Disabled; history tables are used instead.  

---

## Macros
- **Core**: `save_history`, `current_from_history`, `field_lists`, `to_21st_century_date`.  
- **Delivery**: `self_completing_dimensions` fills missing keys in dimensions.  
- **Migrations**: small DDL helpers.  
- **Custom tests**: check default key rules, hash collisions, non-empty models, valid dates.  

---

## Models

### Staging
- `STG_ABC_BANK_*`: standardize input, add defaults, generate surrogate keys and diff hashes.  
- `STG_ABC_BANK_POSITION`: ingests `MARKET_SYNC.SOURCE_DATA.ABC_BANK_POSITION`, normalizes fields, fixes dates.  

### History
- `HIST_*`: incremental history with `save_history`.  
- `HIST_ABC_BANK_POSITION_WITH_CLOSING`: incremental model that closes positions by inserting zeroed rows for disappeared records.  

### Refined
- `REF_*`: current views from history with `current_from_history`.  
- `REF_POSITION_ABC_BANK`: adds unrealized PnL and percentage.  

### Marts (Portfolio)
- Dimensions: account, country, currency, exchange, security (auto-completed with `self_completing_dimensions`).  
- Fact: `FACT_POSITION` joins to dimensions, defines star schema grain, outputs PnL measures.  

### YAMLs
- Apply uniqueness, relationships, accepted values, and hash-collision checks.  

---

## Sources
- **source_abc_bank.yml**: defines positions source with uniqueness and not-null tests.  
- **source_seed.yml**: defines seeds schema.  

---

## Current Value
Implements layered dbt patterns: SCD through history tables, incremental logic for closing positions, default key strategy, custom macros and tests, and a clear star schema for analytics. Documentation, README, and BI integration remain to be built out.
