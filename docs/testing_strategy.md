# Approach to Data Quality and Assurance

Ledgerline concentrates “hard assertions” at two boundaries: **staging** (ingestion edge) and **marts** (contract surface). The goal is to avoid redundant checks that can drift over time (e.g., types/precision enforced one way in staging and differently in marts).

## Guiding principle: declare precision once

All type coercion and numeric precision rules are enforced in **staging SQL** (and validated there). Downstream layers should not recast the same fields again unless there is a business transformation that truly changes meaning.

This keeps the pipeline consistent: one definition of a number is *the* definition.

## Ingestion edge (staging)

Staging models normalize inputs and make the grain explicit. This is where we enforce:

- **Declared grain** (e.g., one row per `customer_code × product_code × plan_code × report_date` for usage).
- **Key validity** (`not_null` on required natural keys and dates).
- **Domain/sanity checks** (e.g., nonnegative usage and prices, valid years).
- **Relational integrity against reference data** (e.g., usage codes must exist in the catalog/CRM seeds, excluding the default `-1` member when applicable).
- **Precision** via explicit casts in staging (e.g., `units_used::number(38,0)`, `unit_price::number(18,6)`), so downstream layers inherit stable types.

If upstream data arrives malformed, duplicated, or incomplete, staging tests fail early and loudly, preventing “quiet corruption” from flowing downstream.

## Seeds (portfolio mode)

This repo is intentionally **seeds-first** for portability and reproducibility.

- Seeds define the input shapes and types (`+column_types` in `dbt_project.yml`).
- Staging tests validate those shapes at the ingestion edge, just like a real system would validate raw ingested tables.

In a production deployment, the metering feed would typically be a **dbt source** with freshness + grain checks; in this portfolio repo, that source is represented as a seed to keep `dbt build` runnable for anyone.

## Refined layer

Refined models are intentionally thin, stable interfaces. They do **not** repeat ingestion logic (dedupe, normalization, precision) that belongs in staging.

Refined exists to publish:

- Consistent naming (`*_key`) for downstream joins.
- Small, stable “surface area” models that marts can depend on.

Testing in refined is minimal by design (lean interfaces, no re-validation of staging rules).

## Consumption layer (marts)

Marts are the contract surface for downstream users. Here we enforce:

- **Schema contracts** (columns + types fixed via dbt contracts).
- **Grain contracts** (e.g., `unique_combination_of_columns` on the fact grain).
- **Business-rule checks** that are specific to the mart output (e.g., pricing logic correctness, derived metrics within expected bounds) when those rules are not already guaranteed upstream.

Importantly, marts do not re-check basic ingestion rules already guaranteed by staging (to avoid drift and duplication).

## External validation

Ledgerline uses a QuerySurge smoke suite that tests:

- **Fact ↔ dimension integrity:** every key in the fact appears in the corresponding dimension.
- **Referential integrity:** no missing country, plan, product or customer codes.
- **Business rules:** e.g., no negative usage; no overage share greater than one.

These are run via dbt tests and an external harness via QuerySurge.

![QuerySurge smoke suite passing](assets/querysurge_v2.jpg)

## Continuous Integration

A GitHub Actions workflow runs `dbt build` on each pull request to validate:

- Staging grain + domain checks (including precision)
- Contract enforcement in marts
- Fact grain uniqueness and other mart-level guarantees

This keeps changes safe, reviewable, and reproducible.
