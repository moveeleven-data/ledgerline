{%- macro self_completing_dimension(
      dim_rel
    , dim_key_column
    , dim_default_key_value = '-1'
    , rel_columns_to_exclude = []
    , fact_defs = []
) -%}

/* ** Usage notes **
 * - The primary key has to be the first field in the underlying reference for the dimension
 */

{% do rel_columns_to_exclude.append(dim_key_column) -%}

with

dim_base as (
    select
          {{ dim_key_column }}
        , d.* EXCLUDE( {{ rel_columns_to_exclude | join(', ') }} )
    from {{ dim_rel }} as d
)

, fact_key_list as ( 
    {% if fact_defs|length > 0 %}   -- If a FACT reference is passed, then check for orphans and add them in the dimension

        {%- for fact_model_key in fact_defs %}
            select distinct {{fact_model_key['key']}} as FOREIGN_KEY
            from {{ ref(fact_model_key['model']) }}
            where {{fact_model_key['key']}} is not null

            {% if not loop.last %}
            union
            {% endif %}

        {%- endfor -%}

    {%- else %}   -- If NO FACT reference is passed, the list of fact keys is just empty.
    select null as FOREIGN_KEY where false
    {%- endif%}
)

, missing_keys as (
    select fkl.FOREIGN_KEY 
    from fact_key_list fkl 
    left outer join dim_base on dim_base.{{dim_key_column}} = fkl.FOREIGN_KEY
    where dim_base.{{dim_key_column}} is null
)

, default_record as (
    select *
    from dim_base
    where {{dim_key_column}} = '{{dim_default_key_value}}'
    limit 1
)

, dim_missing_entries as (
    select 
        mk.FOREIGN_KEY
      , dr.* EXCLUDE( {{ dim_key_column }} )
    from missing_keys as mk 
    join default_record dr
)

, dim as (
    select * from dim_base 
    union all
    select * from dim_missing_entries 
)

select * from dim

{% endmacro %}
