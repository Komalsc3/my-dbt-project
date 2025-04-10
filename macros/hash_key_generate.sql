{%- macro hash_key_generate(table_name, alias="alias") -%}
    {% set all_columns %}
    with cte as(select column_name from {{ source("stage_information_schema", "columns") }} 
        where table_name = upper('{{ table_name }}') ORDER BY ordinal_position)
select listagg(column_name,',')  from cte
    {% endset -%}
    {%- set results = run_query(all_columns) -%}
    {% if execute %}
        {%- set col_string = results.columns[0].values()[0] -%}
        {%- set col_list = col_string.split(",") -%}
        sha2(
            array_to_string(
                array_compact(
                    array_construct(
                        {% for col in col_list %}
                            {{ alias }}.{{ col | trim }}
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                ),
                '|'
            ),
            256
        ) as keyattributehash
    {%- endif -%}
{%- endmacro -%}
