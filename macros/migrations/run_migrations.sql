/** run_migrations.sql
 * This macro is a central place to execute schema/data migrations.
 * Each migration is itself a dbt macro (e.g. V003_drop_table).
 *
 * - To add a migration, call run_migration inside this macro.
 *     Example: {% do run_migration('V003_drop_table', database, schema_prefix) %}
 *
 * - If there are no migrations, log it instead:
 *     {% do log("No migrations to run.", info=True) %}
 *
 * Notes:
 * - Replace 'V003_drop_table' with your actual migration macro name.
 * - Add one run_migration call per migration, in the order they should run.
 * - If no migrations are needed, just log "No migrations to run."
 * - All migrations execute against the given database and schema_prefix.
 */
 
{% macro run_migrations(
        database = target.database
      , schema_prefix = target.schema
) -%}

{% do run_migration('V003_drop_table', database, schema_prefix) %}

{%- endmacro %}