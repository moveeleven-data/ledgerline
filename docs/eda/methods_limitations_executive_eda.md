# Methods & Limitations (Executive EDA)

- **Window definition:** Analysis window set by environment variables `DBT_EDA_START_DATE` and `DBT_EDA_END_DATE` (UTC).  
- **Grain:** One row per subscription × date in `fact_usage_window`.  
- **Minimum volume rule:** Subscriptions must have at least 30 active days to qualify for a recommendation.  
- **Utilization is only defined for plans with non-zero included units. For pay-as-you-go plans
    (included_units = 0), utilization is set to NULL and excluded from averages.
- **Current notes:**  
  - Window contained 13 active subscriptions, each with 30 active days. All subscriptions satisfied the ≥30-day rule in this run.  
  - `days_over_limit` = count of days with `overage_units > 0`. Confirmed with a customer-level spot check. 
  - `limit_hit_ratio` = days_over_limit / active_days. Verified via spot check, always between 0 and 1.
