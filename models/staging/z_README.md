# Staging Layer

Staging is the adaptation layer. It takes raw inputs and transforms them into normalized, deduplicated relations. These relations are then consumed by the history layer to build durable change history and usage spans.


All staging models are **views**. dbt materializes them as lightweight relations in Snowflake so tests and debugging run against real objects with minimal cost.

---

## Purpose

Staging bridges the gap between raw inputs and historical tracking. Its responsibilities include:

1. **Normalize and coerce**  
   - Uppercase all business codes so joins are consistent.  
   - Enforce sane date ranges using `to_21st_century_date`.  
   - Cast numerics into explicit types (e.g., quantities as `number(38,0)`).  

2. **Remove ghost rows and deduplicate**  
   - Drop rows that are completely empty placeholders.  
   - Deduplicate on the natural business grain, keeping the latest record by `ingestion_ts` and using tie-breakers where needed.  

3. **Inject a default member**  
   - Every staging model appends one synthetic record with key `'-1'`.  
   - Attributes are placeholders (`'Missing'`, `0`, or `'unknown'`) with `record_source = 'System.DefaultKey'`.  
   
   This ensures joins never drop rows, even if upstream data is late or incomplete.  

4. **Generate deterministic keys and version hashes**  
   - Surrogate keys (e.g., `customer_hkey = hash(customer_code)`).  
   - Version hashes (e.g., `customer_hdiff = hash(customer_code, customer_name, country_code)`) to detect attribute changes.  
   - Dates are always cast to `YYYY-MM-DD` strings before hashing.  
   - The generic test `hash_collision_free` ensures no two different inputs generate the same hash.  

5. **Add lineage and timing**  
   - Every row gets a stamped load timestamp.
   - `record_source` column tracks the upstream system or identifies synthetic rows.  
   - This makes lineage transparent through history, refined, and marts.  

---

## Inputs

**Seeds**: CRM customers, product catalog, plan catalog, currencies, countries, and price book. In dev/CI these simulate external systems.  

**Source**: `atlas_meter_usage_daily`, declared in `sources.yml`. In dev/CI it points at `_seeds`, in prod it points at raw ingestion tables.  

The same staging SQL works in both environments. Only the schema resolution changes.

---

## Testing Strategy

Tests are declared in `staging.yml` and run against the **view** relations:

- **Uniqueness** at each staging grain (e.g., one row per `(customer, product, plan, date)` in usage).  
- **Not null** and **uniqueness** on codes for reference entities.  
- **Year sanity checks** on dates (e.g., `extract(year from report_date) >= 2000`).  

These tests are intentionally strict. If staging has to “fix” too much bad data, it’s better to surface an error than let broken records bleed downstream. History and refined layers depend on staging being trustworthy. 

---

## Grains & Keys (Facts)

### Usage feed
- **Entity Grain:** customer × product × plan (subscription)  
- **Row Grain:** customer × product × plan × date  
- **Key:** `usage_hkey = hash(subscription, date)`  
- **Diff:** adds `units_used` + `included_units` to capture metric changes

### Daily price book
- **Entity Grain:** product × plan  
- **Row Grain:** product × plan × price_date  
- **Key:** `price_book_hkey = hash(product, plan, date)`  
- **Diff:** adds `unit_price` to capture changes

---

## Time and Timezone

Staging stores all dates and timestamps in UTC.  
- Business dates are treated as simple UTC calendar days.  
- Load timestamps are stamped in UTC when the pipeline runs.  

No time zone conversions are applied in staging. Standardizing everything to UTC avoids regional discrepancies and makes metrics like usage, billing, and churn globally comparable.
