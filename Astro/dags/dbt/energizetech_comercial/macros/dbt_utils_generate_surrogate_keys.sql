{% macro generate_surrogate_key(colunas) %}
    {% set colunas_concatenadas = [] %}
    {% for coluna in colunas %}
        {% do colunas_concatenadas.append("coalesce(cast(" ~ coluna ~ " as varchar), '')") %}
    {% endfor %}
    md5({{ colunas_concatenadas | join(" || '|' || ") }})
{% endmacro %} 
