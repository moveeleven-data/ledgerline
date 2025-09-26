# Dev Utils Macros

This folder contains helpers for local development and iteration.

These macros are meant to speed up testing and debugging, not to shape production data.

Basic guardrails are built in to ensure they only run in dev targets.

---

## Scope

- Write to development tables only, never to marts or production schemas.  
- Provide shortcuts for inserting or removing synthetic rows so you can exercise logic quickly.  
- Make iteration fast during model design, QA, or exploratory work.  

---

## Current State

This folder currently holds simple insert/delete utilities for synthetic usage rows. It may expand with other developer-focused helpers.  

While there are guardrails in place, these macros are strictly for development. Never call them from production models.
