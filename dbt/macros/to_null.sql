{% macro to_null(column_name) %}
    case 
        when {{ column_name }} = '' then null
        when {{ column_name }} ilike 'nan' then null
        when {{ column_name }} ilike 'nat' then null
        when {{ column_name }} ilike 'null' then null
        else {{ column_name }}
    end 
{% endmacro %}