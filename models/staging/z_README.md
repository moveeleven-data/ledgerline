# Staging Layer

Staging is the ingestion edge. It takes raw inputs (seeds and sources) and produces normalized, deduplicated relations with an explicit, trustworthy grain. Downstream layers stay simple because staging guarantees “one row per intended grain.”

All staging models are **views**. dbt materializes them as lightweight relations in Snowflake so tests and debugging run against real objects with minimal cost.

---

## Purpose

Staging exists to make the raw feeds safe and consistent. Its responsibilities include:

1. **Normalize and coerce**
   - Uppercase all business codes so joins are consistent.
   - Enforce sane date ranges using `to_21st_century_date`.
   - Cast numerics into explicit types (e.g., quantities as `number(38,0)`).

2. **Remove ghost rows and deduplicate**
   - Drop rows that are completely empty placeholders.
   - Deduplicate on the natural business grain, keeping the latest record by `load_ts_utc` (with tie-breakers where needed).

3. **Inject a default member**
   - Each staging model appends one synthetic record with key `'-1'`.
   - Attributes are placeholders (`'Missing'`, `0`, or `'unknown'`) with `record_source = 'System.DefaultKey'`.

   This ensures joins never drop rows, even if upstream data is late or incomplete.

4. **Generate deterministic keys (and change hashes where needed)**
   - Surrogate keys (e.g., `customer_hkey = hash(customer_code)`).
   - Optional version hashes (e.g., `customer_hdiff = hash(customer_code, customer_name, country_code)`) when useful for detecting attribute changes.
   - Dates are cast to `YYYY-MM-DD` strings before hashing.

5. **Add lineage and timing**
   - Every row includes a stamped load timestamp (`load_ts_utc`).
   - `record_source` identifies the upstream system or synthetic rows.

---

## Inputs

**Seeds**: CRM customers, product catalog, plan catalog, countries, and price book. In dev/CI these simulate external systems.

**Source**: `atlas_meter_usage_daily`, declared in `sources.yml`. In dev/CI it points at `_seeds`, in prod it points at raw ingestion tables.

The same staging SQL works in both environments. Only schema resolution changes.

---

## Testing Strategy

Tests are declared in `staging.yml` and run against the **view** relations:

- **Uniqueness** at each staging grain (e.g., one row per `(customer, product, plan, date)` in usage).
- **Not null** and **uniqueness** on codes for reference entities.
- **Year sanity checks** on dates (e.g., `extract(year from report_date) >= 2000`).

These tests are intentionally strict. If staging has to “fix” too much bad data, it’s better to surface an error than let broken records bleed downstream. Refined and marts depend on staging being trustworthy.

---

## Grains & Keys (Facts)

### Usage feed
- **Entity Grain:** customer × product × plan (subscription)
- **Row Grain:** customer × product × plan × date
- **Key:** `usage_hkey = hash(subscription, date)`
- **(Optional) Diff Hash:** include metrics (e.g., `units_used`, `included_units`) only if you genuinely use it downstream

### Daily price book
- **Entity Grain:** product × plan
- **Row Grain:** product × plan × price_date
- **Key:** `price_book_hkey = hash(product, plan, date)`

---

## Time and Timezone

Staging stores all dates and timestamps in UTC.

- Business dates are treated as simple UTC calendar days.
- Load timestamps are stamped in UTC when the pipeline runs.

No time zone conversions are applied in staging. Standardizing everything to UTC avoids regional discrepancies and makes metrics like usage and billing globally comparable.
