{{ config(tags = ['qa']) }}

/**
 * qa__synthetic_close_timelines.sql
 * ---------------------------------
 * Pull a small sample of subscriptions (usage_hkeys) that had synthetic CLOSE events,
 * then return all their history rows to analyze the full sequence of changes.
 *
 * Purpose:
 * - Helps validate that synthetic closes are being generated correctly.
 *
 * Output:
 * - Timeline of all events for the sampled keys, ordered by key and date.
 */

{% set qa_top_n_keys = var('qa_top_n_keys', 3) %}

with

top_keys_with_closes as (
    select
        usage_hkey
      , count_if(
            usage_row_type = 'CLOSE_SYNTHETIC'
        ) as num_close_events

    from {{ ref('hist_atlas_meter_usage_daily') }}
    group by usage_hkey
    having
        num_close_events > 0

    qualify row_number() over (
        order by
            num_close_events desc
    ) <= {{ qa_top_n_keys }}
)

select
    history.usage_hkey
  , history.report_date
  , history.usage_row_type
  , history.units_used
  , history.included_units
from {{ ref('hist_atlas_meter_usage_daily') }} as history
join top_keys_with_closes
  on top_keys_with_closes.usage_hkey = history.usage_hkey
order by
    history.usage_hkey
  , history.report_date
  , history.usage_row_type;
