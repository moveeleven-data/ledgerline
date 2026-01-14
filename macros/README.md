# Macros

Ledgerline intentionally keeps macros minimal.

If a macro exists, it must:
1) remove repeated logic in multiple places, and
2) be easier to understand than writing the SQL inline.

## Folder layout

- **`macros/core/`**
  Small, stable utilities used by staging models.

### Core macros

- **`to_21st_century_date(col)`**
  Normalizes date strings where the year is mistakenly prefixed with `00` (e.g. `0021-04-09` → `2021-04-09`).

  Used in staging models that ingest dates from raw feeds.
