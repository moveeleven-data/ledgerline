/**
 * atlas_usage_diff_fields.sql
 * ---------------------------
 * Build the ordered list of fields used to generate usage surrogate hashes.
 *
 * - Always includes business keys:
 *     customer_code, product_code, plan_code, and report_date.
 *
 * - For OPEN rows:
 *     Uses actual column values from staging/history.
 *     Example:
 *       units_used       = 42
 *       included_units   = 100
 *       → Hash = (..., '2025-09-21', 42, 100)
 *
 * - For CLOSE_SYNTHETIC rows:
 *     Replaces usage values with constants (typically 0).
 *     Example:
 *       units_used       = 0
 *       included_units   = 0
 *       → Hash = (..., '2025-09-21', 0, 0)
 *
 *   By forcing the CLOSE row to have a different hash than
 *   the last OPEN row, we guarantee uniqueness.
 */

{% macro ledgerline_usage_diff_fields(
      prefix                     = ''
    , report_date_expr           = "to_varchar(report_date, 'YYYY-MM-DD')"
    , units_used_override        = none
    , included_units_override    = none
) %}

    -- For usage metrics (units_used, included_units),
    -- if an override is provided (e.g. '0' for CLOSE rows), use it.
    -- Otherwise, fall back to the prefixed column (e.g. prior.units_used).

    {%- set units_used_field = units_used_override
         if units_used_override is not none
         else prefix ~ 'units_used'
    -%}

    {%- set included_units_field = included_units_override
         if included_units_override is not none
         else prefix ~ 'included_units'
    -%}

    -- Final ordered list of fields used in surrogate key generation.
    -- Order matters for hash stability.

    {% set fields = [
          prefix ~ 'customer_code'
        , prefix ~ 'product_code'
        , prefix ~ 'plan_code'
        , report_date_expr
        , units_used_field
        , included_units_field
    ] %}

    {{ return(fields) }}

{% endmacro %}
