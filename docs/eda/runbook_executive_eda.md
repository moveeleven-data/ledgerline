# Executive EDA Runbook

**Goal** 
Produce an EDA and executive report that identifies which customers should receive plan recommendations next quarter to increase 90-day revenue while keeping pricing fair.

## Scope
- **Timeframe:** last 90 calendar days (UTC billing day)  
- **Data:** `fact_usage` with `dim_customer`, `dim_product`, `dim_plan`, `dim_currency`, and `dim_country`, using daily price book
- **Outputs:** 4 figures, 1 recommendations CSV, 1 executive report  

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

1. **Action:** Run `dbt build`.  
   **Result:** Models run successfully and all tests pass.  

2. **Action:** Set the window to the last 90 days (UTC).  
   **Result:** Start and end dates are clearly recorded.  
   - start_date = 2025-09-28
   - end_date   = 2025-09-28

3. **Action:** Run `eda_usage_limit_behavior_profile.sql`.  
   **Result:** Counts by product and plan check out and metrics are populated.  

4. **Action:** Export visuals (overage CDF, limit streaks, fairness heatmap).  
   **Result:** Figures are saved with short captions and added to the report.  

5. **Action:** Run `eda_billed_amount_price_sensitivity.sql` and `eda_fairness_by_country_and_plan.sql`.  
   **Result:** Price sensitivity figure is saved and fairness notes are captured, with small-sample regions flagged.  

6. **Action:** Run `eda_plan_change_recommendations_90d.sql`.  
   **Result:** Recommendations CSV is saved and linked in the report.  

7. **Action:** Draft report sections (highlights, recommendations, limitations).  
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
