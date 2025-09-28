/**
eda__usage_limit_behavior_profile.sql
-------------------------------------
Summarize subscription usage and overages from the past 90 days to help guide plan recommendations.

Grain:
- One row per customer–product–plan over a 90-day window

Approach:
- Track activity days, overage days, streaks, utilization, and effective unit price
- Only include subscriptions with enough meaningful activity

Downstream Usage
Figures:
- fig_overage_distribution_cdf.png:  Shows overage patterns by product
- fig_limit_streaks_by_plan.png:     Shows longest overage streaks by plan
- fig_fairness_by_country.png:       Compares usage fairness across countries
*/

select
    max(report_date)
from {{ ref('fact_usage') }}