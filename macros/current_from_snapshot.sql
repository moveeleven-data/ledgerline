{% macro current_from_snapshot(snsh_ref) %}
    select
        * exclude (DBT_SCD_ID, DBT_VALID_FROM, DBT_VALID_TO)
          rename  (DBT_UPDATED_AT as SNSH_load_TS_UTC)
    from {{ snsh_ref }}
    where dbt_valid_to is null
{% endmacro %}