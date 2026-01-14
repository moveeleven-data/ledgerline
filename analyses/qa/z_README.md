# QA Analyses

This folder contains audit probes and diagnostic SQL that validate Ledgerline’s pipeline from staging through marts.

---

## Scope

- Check **referential integrity** - Do facts map to valid dimensions
- Validate **fact grain** - Confirm `fact_usage` respects `(customer_key, product_key, plan_key, report_date)`.
- Recompute **metrics** - Verify billed amounts, included values, and margins against base inputs.  
- Track **row flow** - Confirm counts remain consistent across staging, refined, and marts.  

---

## Current State

Examples include:  
- Grain probes (`qa__fact_usage_grain_probe.sql`)  
- Row count checks (`qa__rowcount_check_by_layer.sql`)  
- Metric recomputation (`qa__fact_usage_metric_check.sql`)  