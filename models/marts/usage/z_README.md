# Usage Mart

The Usage Mart is a **curated star schema**. It publishes conformed dimensions and a daily usage fact table shaped for finance, product, and customer success analytics.  

This layer is what most analysts and BI tools query directly. It translates clean refined data into business-facing structures.

---

## Overall Flow

1. **Refined usage**  
   - `ref_usage_atlas` provides the latest `OPEN` usage at the natural key grain.  
   - Adds `overage_units = greatest(units_used - included_units, 0)`.  

2. **Pricing**  
   - `int__fact_usage_priced` is a view that applies pricing rules.  
   - For each usage row, look up the last effective `unit_price` on or before `report_date` from the daily price book.  
   - Assign a `currency_code` from `var('default_billing_currency', 'USD')`. Multi-currency can be added later by carrying the actual price book currency.  

3. **Dimensions**  
   - **dim_customer, dim_product, dim_plan, dim_country** use `self_completing_dimension`. These start from the refined dimension, then clone the default member for any codes referenced in facts but not yet present.  
   - **dim_currency** is a closed domain. It comes directly from refined and does not self complete. Invalid currency codes should fail tests.  

4. **Fact**  
   - `fact_usage` joins priced usage to dimensions.  
   - Computes `billed_value`, `included_value`, `overage_value`, and `overage_share`.  
   - Publishes one row per enforced grain: `(customer_key, product_key, plan_key, report_date)` (plus `currency_key` if multi-currency is introduced).  

---

## Grain and Contract

- **Fact grain**: `(customer_key, product_key, plan_key, report_date)`.  
- Contracts are enforced in `usage.yml` with uniqueness, column types, and foreign key relationships.  
- If multi-currency is introduced, extend the grain to `(customer_key, product_key, plan_key, report_date, currency_key)`.

---

## Materialization

- **Dimensions**: `table` for fast BI lookups and stable joins.  
- **Intermediate pricing**: `view`. Keeps pricing rules visible and auditable, while the final fact table persists results.  
- **Fact**: `table` for performance and reproducibility.  

This split balances clarity (readable logic in views) with performance (cached facts and dimensions).

---

## Keys and Hashing Decisions

- **Preferred key source**: refined surrogates (`*_hkey`).  
- **Fallback**: if a refined row does not exist but usage references a new code, clone the default dimension row and assign a fallback key `hash(upper(code))`.  

This ensures:  
- Existing rows keep their canonical keys.  
- Fact rows are never lost when usage arrives before catalog refresh.  
- Analysts can still query those facts with a visible “System.DefaultKey” lineage until the true record lands.

---

## Self-Completing Dimensions

- Keeps facts **joinable** even if catalogs are late.  
- Synthetic rows carry `record_source = 'System.DefaultKey'`.  
- On the next refresh, once the real dimension row exists in refined, the mart automatically transitions to the canonical surrogate key.  
- Because joins use natural codes in the pricing view, no fact rows are lost during this transition.  

---

## Testing Strategy

Declared in `usage.yml`:

- **Dimensions**  
  - `not_null` and `unique` on surrogate and business keys.  

- **Fact**  
  - `dbt_utils.unique_combination_of_columns` at the declared grain.  
  - `relationships` tests from each foreign key to its dimension key.  

- **Pricing coverage**  
  - Singular test: error if coverage over the last 7 days < 95%.  
  - Warning: list missing price rows for investigation.  

- **Integrity**  
  - Warn if product codes appear in usage but not in `dim_product`. This is backed up by self-completion, but alerts you that upstream catalogs are lagging.  

---

## Design Decisions

- **Currency handling** is explicit. Default = `USD`. If multi-currency is added, expand the grain and enforce coverage with tests.  
- **Pricing logic as a view**: keeps rules transparent and auditable. If it becomes heavy, promote to a table and add freshness checks.  
- **Contracts**: marts enforce strict contracts so BI tools always query consistent shapes.  

---

## Operational Run Order

1. Seeds load (or upstream sources land).  
2. Staging runs (ephemeral CTEs feeding history).  
3. History updates: dimensions with `save_history`, usage with daily merge and synthetic closes.  
4. Refined collapses history to current.  
5. Marts build: conformed dimensions first, `int__fact_usage_priced`, then `fact_usage`.  

---

## Performance Notes

- Add clustering or micro-partitioning on `report_date` in `fact_usage` if it grows large.  
- Consider incremental builds keyed by `report_date` if pricing or joins become expensive.  

---

## Why This Layer Matters

The Usage Mart is the **business contract**. It turns raw usage into **dollars and dimensions**. Finance teams use it to reconcile billings, product managers use it to measure adoption, and customer success uses it to monitor overages.  

By enforcing contracts, preserving default members, and making self-completion explicit, the mart keeps facts **queryable and trustworthy** — even when upstream systems are late or incomplete.
