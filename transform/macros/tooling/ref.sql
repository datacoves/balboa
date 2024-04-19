{#
    Overrides default ref macro for staging databases, to not include database name.
    This creates relative links to allows swapping of those databases without breaking view references
#}

{% macro ref() %}

    {% set version = kwargs.get('version') or kwargs.get('v') %}
    {% set packagename = none %}
    {%- if (varargs | length) == 1 -%}
        {% set modelname = varargs[0] %}
    {%- else -%}
        {% set packagename = varargs[0] %}
        {% set modelname = varargs[1] %}
    {% endif %}

    {% set rel = None %}
    {% if packagename is not none %}
        {% set rel = builtins.ref(packagename, modelname, version=version) %}
    {% else %}
        {% set rel = builtins.ref(modelname, version=version) %}
    {% endif %}


    {% set db_name = rel.database | lower %}

    {% if db_name.startswith('staging') or db_name.endswith('staging') %}
        {% do return(rel.include(database=false)) %}
    {% else %}
        {% do return(rel) %}
    {% endif %}

{% endmacro %}
