-- This macro overrides dbt's default schema naming behaviour.
-- Default: MART_STAGING, MART_INTERMEDIATE, MART_MART (messy)
-- With this macro: STAGING, INTERMEDIATE, MART (clean)

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}