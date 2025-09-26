# QA Analyses

The `analyses/qa/` folder contains **audit probes and diagnostic SQL** that validate Ledgerline’s pipeline from staging through marts. These files are tagged with `tags: ['qa']` so they can be run selectively.

---

## Scope

- Check **referential integrity** — do facts map to valid dimensions, and are default keys hit unexpectedly?  
- Validate **fact grain** — confirm `fact_usage` respects `(customer_key, product_key, plan_key, report_date)`.  
- Test **continuity** — ensure synthetic closes are generated when subscriptions disappear.  
- Recompute **metrics** — verify billed amounts, included values, and margins against base inputs.  
- Track **row flow** — confirm counts remain consistent across staging, history, refined, and marts.  

---

## Role in the Pipeline

QA analyses are **one-off queries**, not dbt models. They compile to plain SQL in `target/analysis/qa` and can be executed in Snowflake or any SQL client. They provide **visibility and diagnostics**, but do not write back or alter lineage.  

When a probe surfaces an issue, either:  
- Fix the upstream model, or  
- Promote the check into a formal singular test under `tests/`.  

---

## Current State

Examples include:  
- Grain probes (`qa__fact_usage_grain_probe.sql`)  
- Row count checks (`qa__rowcount_check_by_layer.sql`)  
- Default-key hit tracking (`qa__default_hits_by_day.sql`)  
- Synthetic close timelines (`qa__synthetic_close_timelines.sql`)  
- Metric recomputation (`qa__fact_usage_metric_check.sql`)  

---

## Usage

Compile and open:  
```bash
dbt compile --select tag:qa
```

Run these probes regularly in development and before major schema changes. They are your first line of defense in catching drift and data issues early.