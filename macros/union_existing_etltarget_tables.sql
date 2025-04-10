{% macro union_existing_etltarget_tables(base_table_name, servicer) %}
    {% set sql = [] %}
    {% for servicers in servicer %}
        {% set table_name = base_table_name ~ "_" ~ servicers %}
        {% if execute %}
            {% if adapter.get_relation(
                database="stage",
                schema="core",
                identifier=table_name,
            ) %}
                {% do sql.append("select * from " ~ ref(table_name)) %}
            {% endif %}
        {% else %}
            -- During dbt compile
            {% do sql.append("select * from " ~ ref(table_name)) %}
        {% endif %}
    {% endfor %}
    {{ return(sql | join("\nunion all\n")) }}
{% endmacro %}
