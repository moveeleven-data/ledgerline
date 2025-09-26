# Analyses

This folder holds ad-hoc SQL for development, debugging, and QA. These queries are not materialized or in the DAG. dbt compiles them to `target/analysis` for direct execution in the warehouse.  

Use this space for quick experiments, reproducible QA probes, and one-off checks, like validating fact grain and referential integrity, testing continuity with synthetic closes, recomputing usage and billing metrics, and tracking row flow across layers.

---

## Subfolders and Roles

- **`analyses/dev/`**  
  Developer scratch pads and helpers for fast iteration. Examples:  
  - Insert scripts that seed synthetic usage rows to exercise pricing.  
  - Quick joins that visualize staging outputs.  

- **`analyses/qa/`**  
  Repeatable audit probes. Examples:  
  - Row count validations.  
  - Default-key hit analysis.  
  - Duplicate detection at a declared grain.  
  - Pricing coverage checks over a rolling window.