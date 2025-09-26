# Dev Utils Macros

The `macros/dev_utils/` folder contains **helpers for local development and iteration**. These macros are meant to speed up testing and debugging, not to shape production data.

---

## Scope

- Write to **development tables only**, never to marts or production schemas.  
- Provide shortcuts for inserting or removing synthetic rows so you can exercise logic quickly.  
- Make iteration fast during model design, QA, or exploratory work.  

---

## Current State

Right now the folder has simple insert/delete utilities for synthetic usage rows. Over time, it may grow to include other developer-focused helpers.  

These macros are **for development only** — never call them from models that run in production.
