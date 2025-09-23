# Data Dictionary

## Story & Sources

LedgerLine simulates a developer-focused SaaS platform called Atlas. Customers subscribe to products via plans that include some usage and charge overages beyond that.  

Seed CSVs act as the authoritative sources for this demo:

- CRM (customers)  
- Catalog (products and plans)
- Reference (countries and currencies) 
- Price Book (daily per-unit rates by product and plan)  
- Usage Feed (daily metered activity by customer, product, plan, and date) 

We stage these inputs, preserve history, and publish a star schema for analytics.

---

## Seeds (Reference & Source Data)

### atlas_catalog_plan_info - Plan catalog (by product)  
Official list of purchasable plans and their billing period.

| Column       | Type            | Description                                       |
|--------------|-----------------|---------------------------------------------------|
| plan_code    | varchar         | Unique plan identifier (e.g., PLAN-BASIC).        |
| plan_name    | varchar         | Human-readable plan name (e.g., "Basic").         |
| product_code | varchar         | Product this plan belongs to (e.g., PROD-API).    |
| billing_period | varchar       | Billing cadence (e.g., monthly).                  |
| load_ts      | timestamp_ntz   | Timestamp the record was loaded.                  |

**Data quality & handling**  
- plan_code is unique and not null in seed tests.  
- Staging uppercases codes and generates a surrogate key from plan_code.  
- History tracks changes (SCD-style) so plan attributes are time-aware.  

---

### atlas_catalog_product_info - Product catalog  
Authoritative list of products/services offered.

| Column       | Type          | Description                               |
|--------------|---------------|-------------------------------------------|
| product_code | varchar       | Unique product identifier (e.g., PROD-API). |
| product_name | varchar       | Human-readable product name.              |
| category     | varchar       | Product grouping (e.g., platform, data).  |
| load_ts      | timestamp_ntz | Timestamp the record was loaded.          |

**Data quality & handling**  
- product_code is unique and not null in seed tests.  
- Staging uppercases codes and generates a surrogate key from product_code.  
- History captures attribute changes over time.  

---

### atlas_country_info - Country reference  
ISO-like list of countries for customer localization and rollups.

| Column       | Type          | Description                             |
|--------------|---------------|-----------------------------------------|
| country_code | varchar       | Country code (e.g., US, GB).            |
| country_name | varchar       | Country name.                           |
| load_ts      | timestamp_ntz | Timestamp the record was loaded.        |

**Data quality & handling**  
- country_code is unique and not null in seed tests.  
- Staging uppercases codes and generates a surrogate key from country_code.  
- A default "Missing" row is maintained to guarantee joins do not break.  

---

### atlas_crm_customer_info - Customers (from CRM)  
System-of-record for customer identity and geography.

| Column        | Type          | Description                                |
|---------------|---------------|--------------------------------------------|
| customer_code | varchar       | Unique customer identifier (e.g., CUST-001). |
| customer_name | varchar       | Human-readable customer name.              |
| country_code  | varchar       | Country for the customer.                  |
| load_ts       | timestamp_ntz | Timestamp the record was loaded.           |

**Data quality & handling**  
- customer_code is unique and not null in seed tests.  
- Staging uppercases codes and generates a surrogate key from customer_code.  
- Default key strategy ensures referential integrity even when data is incomplete.  

---

### atlas_currency_info - Currency reference  
Supported currencies and display precision.

| Column        | Type          | Description                                    |
|---------------|---------------|------------------------------------------------|
| currency_code | varchar       | Currency code (e.g., USD, JPY).                |
| currency_name | varchar       | Currency name.                                 |
| decimal_digits| number        | Numeric precision for amounts (e.g., 2, 0).    |
| load_ts       | timestamp_ntz | Timestamp the record was loaded.               |

**Data quality & handling**  
- currency_code is unique and not null in seed tests.  
- Staging generates a surrogate key from currency_code.  
- Default key exists to prevent broken joins if currency is missing.  

---

### atlas_meter_usage_daily - Daily metered usage (raw feed)  
Daily units consumed per customer × product × plan × date.

| Column        | Type          | Description                                   |
|---------------|---------------|-----------------------------------------------|
| customer_code | varchar       | Customer that generated usage.                |
| product_code  | varchar       | Product that recorded usage.                  |
| plan_code     | varchar       | Plan in effect when usage occurred.           |
| report_date   | date          | As-of date for the metered usage.             |
| units_used    | number        | Units consumed that day.                      |
| included_units| number        | Units included by the plan that day.          |
| load_ts       | timestamp_ntz | Timestamp the record was loaded.              |

**Data quality & handling**  
- Source freshness is monitored; uniqueness checked at the grain (customer, product, plan, date).  
- Staging fixes:  
  - Uppercases codes; coerces dates into valid ranges.  
  - Filters "all-null ghost rows."  
  - Deduplicates on natural key, keeping the latest by load_ts and higher units_used.  
  - Adds a default row to support default-key integrity patterns.  

---

### atlas_price_book_daily - Daily unit rates (price book)  
Official daily per-unit price by product and plan.

| Column       | Type           | Description                                 |
|--------------|----------------|---------------------------------------------|
| product_code | varchar        | Product for which the rate applies.         |
| plan_code    | varchar        | Plan for which the rate applies.            |
| price_date   | date           | Effective date of the unit price.           |
| unit_price   | number(18,6)   | Price per unit for that day.                |
| load_ts      | timestamp_ntz  | Timestamp the record was loaded.            |

**Data quality & handling**  
- Uniqueness expected at (product_code, plan_code, price_date).  
- Staging uppercases codes and normalizes dates; joins later drive billing math.  
- Numeric type is enforced so prices load as decimals.  

---

## Marts (Star Schema for Analytics)

Dimensions carry surrogate keys (hashed varchar) generated from stable business identifiers. Default "Missing" members ensure referential integrity. Marts are populated from the refined layer after cleaning, deduplication, history capture, and pricing logic.

---

### dim_customer — Customer dimension

| Column        | Type    | Description                                |
|---------------|---------|--------------------------------------------|
| customer_key  | varchar | Surrogate key (from customer_code).        |
| customer_code | varchar | Business identifier from CRM.              |
| customer_name | varchar | Customer name.                             |
| country_code  | varchar | Country code for geography rollups.        |

**Notes**  
Keys generated in staging; history retains changes; default member prevents broken joins.  

---

### dim_product - Product dimension

| Column       | Type    | Description                    |
|--------------|---------|--------------------------------|
| product_key  | varchar | Surrogate key (from product_code). |
| product_code | varchar | Business identifier.           |
| product_name | varchar | Product name.                  |
| category     | varchar | Product grouping.              |

**Notes**  
Built from catalog seed; tracked in history to capture attribute changes.  

---

### dim_plan - Plan dimension

| Column       | Type    | Description                                |
|--------------|---------|--------------------------------------------|
| plan_key     | varchar | Surrogate key (from plan_code).            |
| plan_code    | varchar | Business identifier.                       |
| plan_name    | varchar | Plan name.                                 |
| product_code | varchar | Associated product.                        |
| billing_period | varchar | Billing cadence (e.g., monthly).          |

**Notes**  
Built from plan catalog; history captures renames or cadence changes; default member exists.  

---

### dim_currency - Currency dimension

| Column        | Type    | Description                        |
|---------------|---------|------------------------------------|
| currency_key  | varchar | Surrogate key (from currency_code). |
| currency_code | varchar | Currency code.                     |
| currency_name | varchar | Currency name.                     |
| decimal_digits| number  | Precision for display and rounding. |

**Notes**  
Default currency member prevents broken joins when code is missing.  

---

### dim_country - Country dimension

| Column      | Type    | Description                         |
|-------------|---------|-------------------------------------|
| country_key | varchar | Surrogate key (from country_code).  |
| country_code| varchar | Country code.                       |
| country_name| varchar | Country name.                       |

**Notes**  
Default country member supports complete joins and totals.  

---

### fact_usage - Daily subscription usage and billing  

**Grain:** customer × product × plan × report_date (one row per day per subscription context).

| Column        | Type          | Description                                      |
|---------------|---------------|--------------------------------------------------|
| usage_key     | varchar       | Surrogate key from the natural grain.            |
| customer_key  | varchar       | FK → dim_customer.                               |
| product_key   | varchar       | FK → dim_product.                                |
| plan_key      | varchar       | FK → dim_plan.                                   |
| currency_key  | varchar       | (If configured) FK → dim_currency.               |
| country_key   | varchar       | Derived from customer; FK → dim_country.         |
| report_date   | date          | Usage as-of date.                                |
| units_used    | number        | Units consumed that day.                         |
| included_units| number        | Units bundled by the plan that day.              |
| overage_units | number        | GREATEST(units_used − included_units, 0).        |
| unit_price    | number(18,6)  | From atlas_price_book_daily on (product, plan, date). |
| billed_amount | number(18,6)  | overage_units × unit_price.                      |
| load_ts_utc   | timestamp_ntz | Pipeline load timestamp for lineage/audit.       |

**Notes**  
- Refined layer performs the rate lookup and overage math.  
- Relationship and uniqueness tests protect the grain and referential integrity.  
- If cost inputs are added in the future, margin metrics can be derived downstream.  

---

## How the Pieces Fit

- Price book sets the daily unit price per product × plan.  
- Usage feed reports daily consumption per customer × product × plan.  
- Refined matches each usage row to that day’s price and calculates overages and billed amounts.  
- Marts publish conformed dimensions and fact_usage for finance, product, and leadership analytics.  