# Approach to Data Quality and Assurance

Ledgerline focuses testing at the boundaries: the ingestion edge (staging) and the consumption layer (marts). Rather than scattering checks across the pipeline, the strongest assertions live where data enters the system and where downstream users rely on contract-ready tables.

## Ingestion edge

Staging models normalise source feeds and generate surrogate keys. Here we enforce:

- Correct grain (one record per natural key per day).
- Uniqueness and `not_null` on surrogate keys, natural keys, and dates.
- Accepted values and basic relational integrity (e.g. valid country and currency codes).

If a source feed duplicates usage rows, drops a plan code or arrives incomplete, these tests fail immediately. Early, precise failures surface upstream drift quickly.

## Refined layer

Refined models are intentionally thin wrappers over staging. They:

- Expose a stable, minimal set of attributes (code, name, etc.).
- Publish consistent key naming (`*_key`) for predictable star-schema joins.
- Hide ingestion details that marts should not depend on.

Because refined models are contract surfaces (not transformation playgrounds), they require only basic `not_null` and uniqueness tests on their keys.

## Consumption layer

Marts behave as data contracts. They enforce:

- Schema contracts (columns and types are fixed).
- Grain contracts (unique combinations of dimension keys).
- Business‑rule checks (e.g. no negative usage, inclusion of all price keys).

Any incompatible change fails the build early. This provides strong guarantees to downstream consumers.

## External validation

Ledgerline uses a QuerySurge smoke suite that tests:

- **Fact ↔ dimension integrity:** every key in `fact_usage` appears in the corresponding dimension.
- **Referential integrity:** no missing currency, country, plan, product or customer codes.
- **Business rules:** e.g. no negative usage, no overage share greater than one.

These are run via dbt tests and an external harness via QuerySurge.

![QuerySurge smoke suite passing](assets/querysurge_v2.jpg)

## Continuous Integration

A GitHub Actions workflow runs `dbt build` on each pull request. It connects to Snowflake with a service user and builds staging, refined and marts schemas in a dedicated CI database. This ensures schema contracts, grain checks and referential integrity tests are validated on every change.
