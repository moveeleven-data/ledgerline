# Tests Macros

This folder contains generic test definitions that can be applied from YAML across models.

---

## Scope

- Provide reusable tests for default-key integrity and numeric sanity checks.  
- Keep rules parameterized and explicit so they adapt cleanly across staging, history, refined, and marts.  
- Reduce duplication. All logic for default handling, collisions, and bounds lives here rather than in one-off SQL.  