{% macro union_existing_etltarget_tables(base_table_name, servicer) %}
    {% set sql = [] %}

    {% for servicers in servicer %}
        {% set table_name = base_table_name ~ "_" ~ servicers %}

        {% if execute %}
            {% set relation = adapter.get_relation(
                database=target.database,
                schema=target.schema,
                identifier=table_name
            ) %}

            {% if relation is not none %}
                {% do log("Adding table to union: " ~ table_name, info=True) %}
                {% do sql.append("select * from " ~ ref(table_name)) %}
            {% else %}
                {% do log("Skipping missing table: " ~ table_name, info=True) %}
            {% endif %}
        {% else %}
            -- During dbt compile: include all references (assume tables exist)
            {% do sql.append("select * from " ~ ref(table_name)) %}
        {% endif %}
    {% endfor %}

    {% if sql | length == 0 %}
        {% do log("No valid tables found. Returning fallback dummy query.", info=True) %}
        {{ return("select null as dummy_column where false") }}
    {% else %}
        {{ return(sql | join("\nunion all\n")) }}
    {% endif %}
{% endmacro %}
