# Executive EDA reports

This folder contains the executive-facing write-up and static assets.

- `executive_report_plan_change_recommendations.md` is the report to read first.
- `assets/figures/` holds PNGs referenced by the report.
- `assets/tables/` holds static CSV exports, including `table_plan_change_recommendations_90d.csv`.

Update process
1. Re-run EDA saved queries in dbt Cloud.  
2. Export updated charts and tables into `assets/`.  
3. Refresh links in the report if filenames changed.
