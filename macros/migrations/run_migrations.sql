/** USAGE:
 # -- Adding a migration macro:
 # {#% do run_migration('V003_drop_table', database, schema_prefix) %#}
 #
 # -- How to report there are no migrations to run
 # {#% do log("No migrations to run.", info=True) %#}
 #
 # !! Remove the # from the above lines to uncomment
 # !! Update the string with the macro name !!
 */
 
{% macro run_migrations(
        database = target.database
      , schema_prefix = target.schema
) -%}

{% do run_migration('V003_drop_table', database, schema_prefix) %}

{%- endmacro %}