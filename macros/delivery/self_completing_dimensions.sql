/**
 * self_completing_dimension.sql
 * -----------------------------
 * Ensures that a dimension contains all keys referenced in facts.
 *
 * - Take all rows from the dimension table.
 * - Gather all distinct keys from the fact table.
 * - Find which fact keys don’t exist in the dimension.
 * - For each missing key, copy the default row and swap in that key.
 * - Return the dimension rows plus these new “fill-in” rows.
 *
 * Usage notes:
 * - The primary key column must be the first field in the dimension reference.
 */

{%- macro self_completing_dimension(
      dim_rel
    , dim_key_column
    , dim_default_key_value = '-1'
    , rel_columns_to_exclude = []
    , fact_defs = []
) -%}

{% do rel_columns_to_exclude.append(dim_key_column) -%}


with

/* Step 1. Gather base dimension rows.

   We select every row from the dimension table we are “completing”. */

base_dimension as (
    select
          {{ dim_key_column }}
        , dim_table.* exclude ( {{ rel_columns_to_exclude | join(', ') }} )
    from {{ dim_rel }} as dim_table
)


/* Step 2. Collect fact keys.

   If fact_defs are provided, union distinct keys from each fact model.
   If no fact_defs are passed, return an empty set. */

, fact_keys as (
    {% if fact_defs|length > 0 %}

        {%- for fact_model_key in fact_defs %}

            select distinct 
                {{ fact_model_key['key'] }} as foreign_key
            from {{ ref(fact_model_key['model']) }}
            where {{ fact_model_key['key'] }} is not null

            {% if not loop.last %}
            union                                               -- combine keys from all fact models
            {% endif %}

        {%- endfor -%}

    {%- else %}
        select null as foreign_key                              -- empty set if no facts provided
        where false
    {%- endif %}
)


/* Step 3. Identify missing keys.

   Any fact key that doesn’t find a match in the dimension is “missing”. */

, missing_fact_keys as (
    select fact_keys.foreign_key
    from fact_keys
    left outer join base_dimension
         on base_dimension.{{ dim_key_column }} = fact_keys.foreign_key
    where base_dimension.{{ dim_key_column }} is null
)


/* Step 4. Grab the default row.

   - Every dimension typically has a special default record (e.g. key = -1).
   - We select that row only (LIMIT 1 ensures we only keep one copy). */

, default_dimension_row as (
    select *
    from base_dimension
    where {{ dim_key_column }} = '{{ dim_default_key_value }}'
    limit 1
)


/* Step 5. Build synthetic rows.

   For every missing key found,
     - Copy the default row.
     - Replace its key column with the missing fact key.
     
   This produces a “fill-in” row that makes the dimension complete. */

, synthetic_missing_rows as (
    select 
        missing_fact_keys.foreign_key
      , default_dimension_row.* exclude ( {{ dim_key_column }} )
    from missing_fact_keys
    join default_dimension_row
)


/* Step 6. Combine results.

   Return the union of the base dimension and the synthetic rows. 

   Final output is a complete dimension:
     - All original rows.
     - Plus new rows for every fact key that was missing. */

, completed_dimension as (
    select * from base_dimension
    union all
    select * from synthetic_missing_rows
)

select * from completed_dimension

{% endmacro %}