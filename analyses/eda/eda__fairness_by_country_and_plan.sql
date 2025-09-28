/**
eda__fairness_by_country_and_plan.sql
-------------------------------------
Assess fairness across countries and plans by comparing usage patterns
and unit prices from the past 90 days.

Purpose:
- Spot country–plan combinations that look uneven
- Account for utilization and volume when comparing outcomes

Grain:
- One row per country–plan over a 90-day window
- Can also be broken down by product if needed

Approach:
- Track how many customers are active and how they use their plans
- Compare each country–plan against global averages
- Flag cases that look uneven and have enough data to be reliable

Downstream Usage
- fig_fairness_by_country.png: Heatmap highlighting flagged fairness issues
- Report: Notes flagged cases
*/
