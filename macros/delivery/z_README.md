# Delivery Macros

This folder holds utilities that make models safe for downstream use. Their purpose is to enforce delivery contracts so dimensions and facts remain stable and never drop rows when upstream data lags.

---

## Scope

- Keep facts joinable by applying default-member logic consistently.  
- Distinguish between open domains (customer, product, plan, country) that can self-complete, and closed domains (currency) that must fail fast.  
- Ensure marts and BI tools always resolve every fact to a dimension key.  

---

## Current State

At present the folder contains a single macro for self-completing dimensions. More delivery-focused helpers may be added later as needs expand.
