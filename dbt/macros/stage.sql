{% macro stage(column_name, date_format=null) %}
    case 
        when trim({{ column_name }}) = '' then null
        when trim({{ column_name }}) ilike 'nan' then null
        when trim({{ column_name }}) ilike 'nat' then null
        when trim({{ column_name }}) ilike 'null' then null
        {% if date_format %}
        else TO_DATE(trim({{ column_name }}), '{{ date_format }}')
        {% else %}
        else trim({{ column_name }})
        {% endif %}
    end as {{ column_name | lower |  replace(" ", "_") | replace("'", "") }}
{% endmacro %}

{# Example usage #}
{# For regular columns:
    {{ stage('some_column') }} #}

{# For date columns:
    {{ stage('date_column', 'MM/DD/YYYY') }} #}
