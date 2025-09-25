/**
 * V003_drop_table
 * ----------------
 * Migration macro that drops an obsolete table from the refined layer.
 * - Default target is the current dbt `database` and `schema_prefix`.
 * - Safe to run multiple times thanks to `IF EXISTS`.
 * - Used when refactoring / renaming models, so stale physical tables don’t linger.
 */

{% macro V003_drop_table(
    database = target.database
  , schema_prefix = target.schema
) -%}

drop table if exists {{ target_database }}.{{ schema_prefix }}_refined.ref_country_atlas;
    
{%- endmacro %}