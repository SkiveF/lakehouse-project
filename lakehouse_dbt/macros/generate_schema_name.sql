-- macros/generate_schema_name.sql
-- Surcharge le comportement par défaut de dbt pour utiliser
-- le nom de schema tel quel (silver → silver, gold → gold)
-- sans préfixe "lakehouse_dbt_"

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

