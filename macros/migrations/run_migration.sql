/** run_migrations.sql
 * -------------------
 * Runs a migration macro by name with (database, schema_prefix).
 * Logs the call, looks up the macro in context, executes it if found,
 * otherwise logs a skip. Wrapped in `if execute` to avoid running during docs.
 */

{% macro run_migration(migration_name, target_database, schema_prefix) %}

    {% if execute %}

        {{ log(
            "Running migration: " ~ migration_name
            ~ " (database = " ~ target_database
            ~ ", schema_prefix = " ~ schema_prefix ~ ")",
            info=true
        ) }}

        {% set migration_macro = context.get(migration_name, none) %}

        {% if migration_macro %}
            {{ run_query(migration_macro(target_database, schema_prefix)) }}
        {% else %}
            {{ log("!! Migration macro " ~ migration_name ~ " not found. Skipping.", info=true) }}
        {% endif %}

    {% endif %}

{% endmacro %}