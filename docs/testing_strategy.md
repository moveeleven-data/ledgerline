# Testing Strategy

In Ledgerline I use a testing strategy that focuses on protecting the boundaries of the data flow, rather than exhaustively testing every transformation step in the middle. That means the strongest tests live where data enters the system and where data is consumed. At the ingestion edge, I validate grain, uniqueness, nullability, accepted values, and basic relational integrity so that upstream drift shows up immediately. If the source feed duplicates usage rows, if a catalog file drops a plan code, or if a price book arrives incomplete, those issues surface right away. This follows Zagni’s principle that early, precise failures are more valuable than sprawling test suites scattered throughout intermediate layers.

In the history models, I validate that each natural key produces a stable surrogate key, that version hashes behave consistently, and that no unexpected duplicates appear at the grain of (usage_hkey, report_date). Because Ledgerline’s history is append-only, the goal is simply to ensure changes are detected correctly and that incremental loads behave predictably. In the refined layer, I check that surrogate keys and natural keys stay aligned, that default keys don’t leak in unexpectedly, and that dimensions remain referentially clean.

At the consumption side, I tighten validation because these models behave as data contracts for the outside world. Mart models enforce schema contracts and grain contracts (unique combinations of the dimension keys). Any incompatible change fails the build early. The overall strategy is strong, defensive tests at the edges; structural and rule-based checks in the middle; and contract-level guarantees at the end. It scales across projects because it respects the natural flow of data and puts testing effort exactly where it catches the most meaningful failures.

---

## External Data Testing with QuerySurge

On top of dbt’s built-in tests, Ledgerline uses QuerySurge as an external test harness around the Snowflake warehouse. A dedicated, read-only Snowflake service user (key-pair authenticated, no MFA) connects QuerySurge to the `LEDGER_LINE_DEV` database and runs a focused smoke suite against the core Ledgerline models.

The **`Ledgerline_Smoke_Suite`** currently covers three main areas:

- **History → refined reconciliation**  
  A flagship QueryPair (`HistLatest_vs_Ref_Usage_Monthly`) recomputes the “latest row per usage key” directly from the history table and compares monthly counts and sums to the refined usage view. This confirms that the history logic and refined layer stay in sync.

- **Fact → dimension integrity**  
  A small cluster of QueryPairs checks that every key in `FACT_USAGE` has a matching row in the core dimensions (customer, product, plan, currency, country). These are simple distinct-key comparisons that catch missing or mis-joined dimension rows early.

- **Business-rule guards**  
  Additional QueryPairs enforce non-negotiable rules like “no negative usage.” These are written so that any violating rows show up directly in the QuerySurge result grid for quick inspection.

I run this suite via a small script that runs `dbt run` and `dbt test` and then triggers `Ledgerline_Smoke_Suite` through the QuerySurge CLI.

![QuerySurge smoke suite passing](assets/querysurge_v2.jpg)

---

## Continuous Integration with GitHub Actions

Ledgerline’s dbt tests run automatically in CI. A GitHub Actions workflow is triggered on pull requests to `main`. The workflow connects to Snowflake using a key-pair–authenticated service user defined via encrypted repository secrets. In CI, `dbt build` runs end-to-end against a set of dedicated CI schemas in `LEDGER_LINE_DEV` (for example, `DBT_MTRIPODI_CI_STAGING`, `DBT_MTRIPODI_CI_HISTORY`, `DBT_MTRIPODI_CI_REFINED`, and `DBT_MTRIPODI_CI_MARTS_USAGE`). Seeds, models, and tests are all executed there, so schema contracts, grain checks, and referential integrity tests are run on every change.
