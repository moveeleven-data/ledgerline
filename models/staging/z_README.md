# Staging Layer

Staging is the **adaptation layer**. It takes raw inputs (from seeds in dev/CI or true sources in prod) and transforms them into **normalized, deduplicated, default-safe relations**. These relations are then consumed by the history layer to build durable SCDs and usage spans.

All staging models are `materialized: ephemeral`. dbt inlines them as CTEs into downstream SQL, so they never persist in Snowflake. This keeps warehouse cost down and avoids clutter while still giving you clear, testable model files.

---

## Purpose

Staging bridges the gap between raw inputs and historical tracking. Its responsibilities include:

1. **Normalize and coerce**  
   - Uppercase all business codes so joins are consistent.  
   - Enforce sane date ranges using `to_21st_century_date`.  
   - Cast numerics into explicit types (e.g., quantities as `number(38,0)`).  

2. **Remove ghost rows and deduplicate**  
   - Drop rows that are completely empty placeholders.  
   - Deduplicate on the natural business grain, keeping the latest record by `load_ts` and using tie-breakers where needed.  
     - Usage feed: `(customer_code, product_code, plan_code, report_date)`  
     - Price book: `(product_code, plan_code, price_date)`  

3. **Inject a default member**  
   - Every staging model appends exactly one synthetic record with key `'-1'`.  
   - Attributes are placeholders (`'Missing'`, `0`, or `'unknown'`) with `record_source = 'System.DefaultKey'`.  
   - This ensures joins never drop rows, even if upstream data is late or incomplete.  

4. **Generate deterministic keys and version hashes**  
   - Surrogate keys (e.g., `customer_hkey = hash(customer_code)`).  
   - Version hashes (e.g., `customer_hdiff = hash(customer_code, customer_name, country_code2)`) to detect attribute changes.  
   - Usage staging adds both:  
     - `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`  
     - `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`  
   - Dates are always cast to `YYYY-MM-DD` strings before hashing to prevent adapter-specific quirks.  
   - The generic test `hash_collision_free` ensures no two different inputs generate the same hash.  

5. **Add lineage and timing**  
   - Every row gets a stamped `load_ts_utc = {{ run_started_at }}`.  
   - `record_source` column tracks the upstream system or identifies synthetic rows.  
   - This makes lineage transparent through history, refined, and marts.  

---

## Inputs

- **Seeds**: CRM customers, product catalog, plan catalog, currencies, countries, and price book. In dev/CI these simulate external systems.  
- **Source**: `atlas_meter_usage_daily`, declared in `sources.yml`. In dev/CI it points at `_seeds`, in prod it points at raw ingestion tables.  

The same staging SQL works in both environments. Only the schema resolution changes.

---

## Materialization Choice

`+materialized: ephemeral` is deliberate. Staging is **not a reporting layer**. Its job is to clean inputs, add hashes, and pass them forward. Persisting staging tables would waste compute and storage without adding value. Ephemeral models give you:

- Explicit, testable logic in named model files.  
- Cost savings (no intermediate tables).  
- A clean warehouse surface — no proliferation of half-finished tables.  

---

## Testing Strategy

Tests are declared in `staging.yml`. They include:

- **Uniqueness** at each staging grain (e.g., one row per `(customer, product, plan, date)` in usage).  
- **Not null** and **uniqueness** on codes for reference entities.  
- **Year sanity checks** on dates (e.g., `extract(year from report_date) >= 2000`).  

These tests are intentionally strict. If staging has to “fix” too much bad data, it’s better to surface an error than let broken records bleed downstream. History and refined layers depend on staging being trustworthy.

---

## Self-Completing Dimensions (Connection)

Staging provides consistent codes and inject one default row. Later, in the marts layer, macros like `self_completing_dimension` compare facts against dimensions and synthesize any missing keys.  

By ensuring all staging codes are uppercase, normalized, and have a default row, we make sure those self-completion joins are clean and predictable.

---

## Operational Notes

- Keep staging CTEs **small and readable**. Favor clarity over cleverness.  
- Do not overuse defaults — they exist to keep joins working, not to hide upstream errors. Downstream tests will highlight excessive reliance on default rows.  
- Treat staging models as **contracts**: they enforce a stable shape for history. Any upstream change must first adapt here.  

---

## Why This Layer Matters

Without staging, downstream models would constantly re-implement normalization, deduplication, and key generation. That would be both expensive and brittle. Staging provides a **single, centralized place** for all adaptation logic, ensuring:

- Stable surrogate keys  
- Clean change-tracking in history  
- Safe joins in marts  
- Lower warehouse costs  

It is the quiet backbone of the Ledgerline pipeline.
