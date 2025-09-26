# Core Macros

The `macros/core/` folder contains **universal utilities** that are safe to use anywhere in the project. These macros are pure, deterministic, and side-effect free.

---

## Scope

- Provide small cleanup and normalization helpers.  
- Stay **minimal and generic** — no assumptions about Atlas schemas.  
- Always deterministic, so the same input yields the same output.  
- Free of database writes or stateful behavior.  

---

## Current State

This folder is intentionally small. As the project grows, add only **lightweight, universal helpers** here that improve clarity without introducing side effects.
