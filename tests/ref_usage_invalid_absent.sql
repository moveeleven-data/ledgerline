/**
 * ref_usage_no_invalid_leaks.sql
 * ------------------------------
 * Errors if any rows flagged as invalid (`ref_usage_atlas__invalid`)
 * are still present in the clean reference table (`ref_usage_atlas`).
 *
 * Purpose:
 * Ensures quarantine logic is respected. Invalid usage rows
 * must be filtered out before making it into analytics-ready data.
 */

{{ config(tags=['qa'], severity='error') }}

with

invalid_usage_rows as (
    select
        usage_hkey
    from {{ ref('ref_usage_atlas__invalid') }}
)

, leaked_invalid_usage as (
    select
        clean.usage_hkey
    from {{ ref('ref_usage_atlas') }} as clean
    inner join invalid_usage_rows as invalid
        using (usage_hkey)
)

select
    *
from leaked_invalid_usage