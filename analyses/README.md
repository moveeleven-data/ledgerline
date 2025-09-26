# Analyses Layer

The `analyses/` folder holds **ad-hoc SQL** that supports development, debugging, and quality review. Unlike models, these files are **not materialized** and do not participate in lineage. Instead, dbt compiles them into `target/analysis`, where you can run them directly in your warehouse.

This is the right place for **investigative SQL, reproducible probes, and one-off fixes** you want under version control without turning them into models.

---

## Purpose

- Give developers a scratch space for quick experiments.  
- Provide QA partners with repeatable checks for row counts, default-key usage, pricing coverage, and more.  
- Allow one-off data probes to be shared and versioned without polluting the model DAG.  

---

## Subfolders and Roles

- **`analyses/dev/`**  
  Developer scratch pads and helpers for fast iteration. Examples:  
  - Insert scripts that seed synthetic usage rows to exercise pricing.  
  - Quick joins that visualize staging outputs.  

- **`analyses/qa/`**  
  Repeatable audit probes tagged with `tags: ['qa']`. Examples:  
  - Row count validations.  
  - Default-key hit analysis.  
  - Duplicate detection at a declared grain.  
  - Pricing coverage checks over a rolling window.  

This split keeps experiments separate from audits and makes QA probes selectable as a group.

---

## How Analyses Connect to the Pipeline

Analyses target the same contracts used by models. They can query across all layers:

- **Sources and seeds** — e.g., `atlas_meter_usage_daily`, catalog and reference seeds.  
- **Staging** — normalized codes, default rows added, ghost rows removed.  
- **History** — daily `OPEN` rows plus synthetic `CLOSE` rows.  
- **Refined** — latest current state, with convenience fields like `overage_units`.  
- **Marts** — conformed dimensions and `fact_usage`.  

Because analyses are **read-only**, they never write back. If you need a permanent check, move the logic into `tests/` or a model.

---

## Patterns Used Here

- **As-of controls**  
  Many QA analyses accept `var('as_of_date')`, defaulting to `run_started_at`. This makes probes reproducible and backfillable.  

- **Default-key accounting**  
  Probes count rows that resolve to the default dimension member or fail to join at all. This highlights where self-completing dimensions are patching gaps.  

- **Pricing coverage**  
  Probes recompute metrics or coverage rates across a window. This helps detect backfills or late price book arrivals.  

---

## Running Analyses

1. Compile everything with `dbt compile`.  
2. Open a specific file in your SQL client from `target/analysis`, or copy the SQL directly from the repo.  
3. Run it against your warehouse.  

Analyses are lightweight, safe, and repeatable. They provide quick insights without changing the DAG.

---

## Why This Layer Matters

Analyses give you a **sandbox for exploration** and a **toolbox for QA**. They help you understand pipeline behavior, catch upstream issues, and experiment with ideas. By keeping them versioned and organized, you maintain a clear boundary: **models define production logic, analyses provide insight and validation**.
