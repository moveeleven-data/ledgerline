# EDA saved analyses

Purpose: Parameter-free, reproducible SQL used to answer the primary question for this phase.

Contents
- `eda_usage_limit_behavior_profile.sql` builds per subscription metrics for limits, utilization, and price exposure.
- `eda_billed_amount_price_sensitivity.sql` measures billed_amount sensitivity to unit_price and price book changes.
- `eda_fairness_by_country_and_plan.sql` checks fairness across geographies and plans.
- `eda_plan_change_recommendations_90d.sql` produces the final candidate list with action and 90 day impact.

Run notes
- Execute these from dbt Cloud’s **Saved queries**.  
- Outputs are used to create figures and tables under `reports/assets/`.