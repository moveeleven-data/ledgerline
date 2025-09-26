# Usage Mart

The Usage Mart is a curated star schema.

It publishes conformed dimensions and a daily usage fact table shaped for finance, product, and customer success analytics.  

This layer is what most analysts and BI tools query directly. It translates clean refined data into business-facing structures.

---

## Overall Flow

1. **Usage input**  
   Pull the latest daily usage at the natural key grain, with convenience fields like overage units added.  
   *(model: `ref_usage_atlas`)*  

2. **Pricing**  
   Apply pricing rules using the daily price book. For each row, assign the last valid unit price on or before the report date and apply a billing currency.  
   *(model: `int__fact_usage_priced`)*  

3. **Dimensions**  
   Bring in customer, product, plan, country, and currency dimensions so every fact row has a matching key.  
   *(models: `dim_customer`, `dim_product`, `dim_plan`, `dim_country`, `dim_currency`)*  

4. **Fact table**  
   Join priced usage to dimensions, calculate billing values, and publish one row per enforced grain (customer, product, plan, date, plus currency if enabled).  
   *(model: `fact_usage`)*  

---

## Grain and Contract

- The fact table is stored at the daily level for each customer, product, and plan.  
- Data contracts enforce rules like uniqueness, correct column types, and valid relationships to dimensions.  
- If multiple currencies are introduced, we will expand the grain to include currency as well, so each row is uniquely defined by customer, product, plan, date, and currency.  

---

## Materialization

- Dimensions are stored as tables so they can be joined quickly and consistently in BI tools.  
- The intermediate pricing step is kept as a view, which makes the pricing rules easy to read and audit without materializing extra data.  
- The final fact table is stored as a table for performance and reproducibility.  

This balance keeps the logic transparent while ensuring that the heaviest joins are precomputed and cached.  

---

## Keys and Self-Completing Dimensions

In the usage mart, facts normally link to surrogate keys that flow through from the refined layer. These are the stable identifiers created in staging and carried forward into refined dimensions.  

Sometimes usage arrives with a code (customer, product, plan, or country) that isn’t in refined yet. Instead of dropping that row, the mart creates a placeholder by cloning the default dimension member. It then applies the same surrogate key recipe defined in staging for that dimension. Because these recipes are deterministic, the placeholder row gets the exact same key it will have once the real record lands.  

These rows are clearly marked with `System.DefaultKey`, making it obvious that they came from a placeholder. When the true record eventually shows up in refined, the mart automatically switches over to the canonical row without losing any facts or changing their identity.

This “self-completing” pattern applies to customer, product, plan, and country. It ensures that facts always have a dimension row to join to, even if reference catalogs are late. Currency is handled differently, because valid currencies come from a fixed list, no placeholders are created. If a code is missing or invalid, the row fails validation so the issue is visible right away.

---

## Testing Strategy

Declared in `usage.yml`:

**Dimensions**  
- `not_null` and `unique` on surrogate and business keys.  

**Fact**  
- `dbt_utils.unique_combination_of_columns` at the declared grain.  
- `relationships` tests from each foreign key to its dimension key.  

**Pricing coverage**  
- Singular test: error if coverage over the last 7 days < 95%.  
- Warning: list missing price rows for investigation.  

**Integrity**  
- Warn if product codes appear in usage but not in `dim_product`. This is backed up by self-completion, but alerts you that upstream catalogs are lagging.  

---

## Operational Run Order

1. Seeds load (or upstream sources land).  
2. Staging runs (ephemeral CTEs feeding history).  
3. History updates dimensions with `save_history`, usage with daily merge and synthetic closes.  
4. Refined collapses history to current.  
5. Marts build conformed dimensions first, `int__fact_usage_priced`, then `fact_usage`.  