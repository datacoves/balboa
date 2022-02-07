{# 
    Overrides default ref macro for staging databases, to not include database name in ref tag.
    This creates relative links to allows swapping of those databases without breaking view references 
    #}

{% macro ref(modelname) %}
    {% if builtins.ref(modelname).database.startswith('staging') %}
        {{ builtins.ref(modelname).include(database=False).render() }}
    {% else %}
        {{ builtins.ref(modelname) }}
    {% endif %}
{% endmacro %}