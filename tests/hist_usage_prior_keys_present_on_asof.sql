/**
 * hist_usage_prior_keys_present_on_asof.sql
 * -----------------------------------------
 * Errors if subscriptions that were OPEN before the as-of date
 * have no row on the as-of date (orphaned keys).
 *
 * Purpose:
 * Ensures continuity in the history table. An OPEN subscription yesterday
 * must still appear today, either as still OPEN or properly closed.
 * Missing rows break churn and carry-forward logic.
 */

{{ config(tags=['qa'], severity='error') }}

{% set as_of_str  = get_latest_usage_report_date() %}
{% set as_of_date = "to_date('" ~ as_of_str ~ "')" %}

with

open_subscriptions_prior as (
    select distinct
          customer_code
        , product_code
        , plan_code
    from {{ ref('hist_atlas_meter_usage_daily') }}
    where
          usage_row_type = 'OPEN'
      and report_date < {{ as_of_date }}
)

, subscriptions_asof as (
    select distinct
          customer_code
        , product_code
        , plan_code
    from {{ ref('hist_atlas_meter_usage_daily') }}
    where
        report_date = {{ as_of_date }}
)

, orphaned_subscriptions as (
    select
          prior.customer_code
        , prior.product_code
        , prior.plan_code
    from open_subscriptions_prior as prior
    left join subscriptions_asof as asof
        using (customer_code, product_code, plan_code)
    where
        asof.customer_code is null
)

select
    *
from orphaned_subscriptions