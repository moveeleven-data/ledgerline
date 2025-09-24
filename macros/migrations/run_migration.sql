/** run_migrations.sql
 * -------------------
 * Runs a migration macro by name with (database, schema_prefix).
 * Logs the call, looks up the macro in context, executes it if found,
 * otherwise logs a skip. Wrapped in `if execute` to avoid running during docs.
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