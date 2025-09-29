# Plan Change Recommendation EDA Runbook

**Goal** 
Produce an EDA and report that identifies which customers should receive plan recommendations next quarter to increase 90-day revenue while keeping pricing fair.

## Scope
- **Timeframe:** last 90 calendar days (UTC billing day)  
- **Data:** `fact_usage` with `dim_customer`, `dim_product`, `dim_plan`, `dim_currency`, and `dim_country`, using daily price book
- **Outputs:** 4 figures, 1 recommendations CSV, 1 report  

## Outputs
- **Figures:**  
  - `fig_overage_distribution_cdf.png`  
  - `fig_limit_streaks_by_plan.png`  
  - `fig_price_sensitivity_by_product.png`  
  - `fig_fairness_by_country.png`  
- **Table:** `table_plan_change_recommendations_90d.csv`  
- **Report:** `executive_report_plan_change_recommendations.md`  

## Saved Analyses
- `eda_usage_limit_behavior_profile.sql`  
- `eda_billed_amount_price_sensitivity.sql`  
- `eda_fairness_by_country_and_plan.sql`  
- `eda_plan_change_recommendations_90d.sql`  

## Steps

## Steps 

1. **Action:** Run `dbt build --select marts.usage --exclude marts.usage.eda`.  
   **Result:** Dimensions and the canonical `fact_usage` build and all tests pass.  
   *Note: `fact_usage` only keeps the latest day (finance-friendly). For time-series EDA, use the windowed models below.*   

2. **Action:** Set the EDA date window (UTC).  
   **Result:** Start and end dates are recorded for reproducibility.  
   - Use environment variables:  
     - `DBT_EDA_START_DATE` = 2025-09-22  
     - `DBT_EDA_END_DATE`   = 2025-09-28  

3. **Action:** Build the windowed EDA models.  
   **Command:** `dbt build --select +marts.usage.eda`  
   **Result:** `int__fact_usage_priced_window` and `fact_usage_window` materialize for the chosen range.  

4. **Action:** Run `eda_usage_limit_behavior_profile.sql`.  
   **Expected Result:** Counts by product and plan check out and metrics are populated.  
   **Actual Result:**
     - 13 subscriptions were active between 2025-09-22 and 2025-09-28 (stable at 13 rows per day)
     - Total: 91 rows across 7-day window
     - days_over_limit metric validated: subscriptions show expected variation (0, partial, 30).
          Spot check on customer_key '8a12...' confirmed 30/30 over-limit days.
     - limit_hit_ratio values calculated for all 13 subscriptions.
          Spot check shows values between 0 and 1, consistent with expectations.
          (0 = never over, 1 = always over)
     - `utilization` metric calculated, NULL where `included_units = 0`.  
          Spot check confirms expected behavior: < 1.0 = under limit, 1.0 = at limit, > 1.0 = over.
     - streak_days_over_limit validated. Confirmed always less than or equal to days_over_limit.

5. **Action:** Export visuals (overage CDF, limit streaks, fairness heatmap).  
   **Result:** Figures are saved with short captions and added to the report.  

6. **Action:** Run `eda_billed_amount_price_sensitivity.sql` and `eda_fairness_by_country_and_plan.sql`.  
   **Result:** Price sensitivity figure is saved and fairness notes are captured, with small-sample regions flagged.  

7. **Action:** Run `eda_plan_change_recommendations_90d.sql`.  
   **Result:** Recommendations CSV is saved and linked in the report.  

8. **Action:** Draft report sections (highlights, recommendations, limitations).  
   **Result:** Initial version of the executive report is ready for review.  
   

## Acceptance Checks
- Figures can be reproduced and filenames match.  
- Recommendations table has `recommended_action`, `expected_revenue_delta_90d`, and a short rationale.  
- Minimum activity rule applied (for example, 30+ active days).  
- Price volatility flagged if it shows up.  

## Limitations
- The analysis does not include costs, so profitability is outside the scope.  
- Customer churn is not modeled. Recommendations focus only on right-sizing plans.  
- The daily price book is assumed to be accurate and is treated as the single source of truth.  
