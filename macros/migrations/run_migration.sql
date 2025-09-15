/**
 * == Helper macro ==
 * It that takes the name of the macro to be run, db and schema and runs it with logs.
 * It uses the context to get the function object from its name
 */
{% macro run_migration(migration_name, database, schema_prefix) %}
{% if execute %}
    {% do log("Running " ~ migration_name ~ " migration with database = "
            ~ database ~ ", schema_prefix = " ~ schema_prefix, info=True) %}

    {% set migration_macro = context.get(migration_name, none) %}
    {% do run_query(migration_macro(database, schema_prefix)) if migration_macro
          else log("!! Macro " ~ migration_name ~ " not found. Skipping call.", info=True) %}
{% endif %}
{% endmacro %}