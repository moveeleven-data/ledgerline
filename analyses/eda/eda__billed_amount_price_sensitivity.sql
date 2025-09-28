/**
eda__billed_amount_price_sensitivity.sql
----------------------------------------
Analyze how billed amounts respond to price changes over the
last 90 days, separating true usage pressure from volatility.

Purpose:
- Measure sensitivity of billed amounts to price shifts
- Flag products and plans as stable, moderate, or volatile
- Compare actual spend with simple what-if scenarios

Grain:
- One row per product–plan, aggregated over 90 days

Approach:
- Track how often prices change
- Compare actual billed amounts with small price adjustments
- Label each product–plan as stable, moderate, or volatile

Downstream Usage
- fig_price_sensitivity_by_product.png: Shows products grouped by plan with stability flagged
- Report: Includes a note on whether price changes may bias recommendations
*/
