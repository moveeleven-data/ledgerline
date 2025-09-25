/** run_migrations.sql
 * -------------------
 * Central entry point to execute schema or data migrations.
 * Each migration is a dbt macro, for example V003_drop_table.
 *
 * Usage (examples, not executed):
 * - Add a migration call, e.g.: run_migration('V003_drop_table', database, schema_prefix)
 * - If there are no migrations, log it, e.g.: log("No migrations to run.", info=true)
 *
 * Notes:
 * - Add one migration call per version, in run order.
 * - All migrations execute against the given database and schema_prefix.
 */
 
{% macro run_migrations(
      database = target.database
    , schema_prefix = target.schema
) -%}

    {{ log("Running migrations...", info=true) }}

    {{ run_migration('V003_drop_table', database, schema_prefix) }}

    {{ log("Finished migrations.", info=true) }}

{%- endmacro %}
