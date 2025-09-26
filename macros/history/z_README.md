# History Macros

The `macros/history/` folder contains the **toolkit for persisting state over time**. These macros define how Ledgerline captures change, generates synthetic closes, and selects current rows.

---

## Scope

- Encode **SCD2 patterns** and reference history logic.  
- Generate **synthetic closes** to explicitly represent churn.  
- Provide helpers for **as-of resolution** and prior-open lookups.  
- Ensure hashing and diff logic are **deterministic and consistent** across usage and reference history.  

---

## Role in the Pipeline

History macros work together to:  
- Turn staging outputs into durable daily logs.  
- Guarantee every open key is eventually closed.  
- Support downstream refined models by exposing both current and historical views.  

They are the backbone of Ledgerline’s **temporal integrity**: ensuring not just what is true now, but what was true at any point in time.

---

## Current State

This folder already includes utilities for hashing, as-of resolution, prior-open probing, synthetic closes, and saving history. Over time, additional history helpers may be added, but the guiding principle remains: **make change explicit and reproducible**.
