# Delivery Macros

The `macros/delivery/` folder contains utilities that **stabilize models for downstream use**. These macros enforce delivery contracts so dimensions and facts remain consumable even when upstream catalogs lag.

---

## Scope

- Guarantee dimensions never drop fact rows.  
- Apply **default-member logic** consistently so synthetic rows are safe and traceable.  
- Distinguish between **open domains** (customer, product, plan, country) that can self-complete and **closed domains** (currency) that must fail fast.  
- Keep marts and BI tools stable by ensuring every fact resolves to a dimension key.  

Macros here operate only in the **presentation layer**. They don’t create facts or history — they **make delivered tables reliable**.

---

## Current State

At present this folder has a single macro, but it will expand as delivery needs grow (e.g., bridge shaping, rollup helpers).
