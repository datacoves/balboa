{# 
    Overrides default ref macro for staging databases, to not include database name in ref tag.
    This creates relative links to allows swapping of those databases without breaking view references 
    #}

{% macro ref(modelname) %}
    {% set db_name = builtins.ref(modelname).database | lower %}}
    {% if db_name.startswith('staging') or 
        db_name.endswith('staging')  %}
        {{ return(builtins.ref(modelname).include(database=false)) }}
    {% else %}
        {{ return(builtins.ref(modelname)) }}
    {% endif %}
{% endmacro %}
