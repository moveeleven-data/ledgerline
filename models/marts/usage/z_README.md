# Usage Mart

The Usage Mart is a lean star schema built Refined layer.  
Facts hash natural keys into surrogate keys; dimensions pass through from REF.

---

## Models

- **Dims:** `dim_customer`, `dim_product`, `dim_plan`, `dim_currency`, `dim_country`  
- **Intermediate:** `int_fact_usage_priced` (prices usage at NK grain)  
- **Facts:** `fact_usage`, `eda/fact_usage_window`

---

## Grain & Keys

Fact grain = `customer_key × product_key × plan_key × report_date`  
Keys = deterministic hashes from NKs (no joins to dims).

---

## Materialization

- Dims → tables  
- Intermediate → view  
- Facts → tables  

---

## Testing

`not_null`, `unique`, and FK `relationships` to each dim key.

---

## Layout

marts/

usage/
dim_customer.sql
dim_product.sql
dim_plan.sql
dim_currency.sql
dim_country.sql
fact_usage.sql

intermediate/
int_fact_usage_priced.sql

eda/
int_fact_usage_priced_window.sql
fact_usage_window.sql