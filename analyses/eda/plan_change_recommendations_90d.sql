/**
plan_change_recommendations_90d.sql
----------------------------------------
Build a ranked list of customer–product pairs for plan
recommendations over the next quarter. The aim is to
increase revenue and improve fairness through right-sizing,
with customer consent.

Purpose:
- Score subscriptions based on recent usage patterns
- Recommend actions such as upsell, adjust units, down-tier, or hold
- Estimate potential revenue impact

Grain:
- One row per customer–product over a 90-day window

Approach:
- Only include subscriptions with enough meaningful activity
- Identify candidates for upsell, adjustment, or down-tier based on usage
- Estimate revenue impact from simple plan changes
- Prioritize actions (upsell first, then adjust, then down-tier, else hold)
*/