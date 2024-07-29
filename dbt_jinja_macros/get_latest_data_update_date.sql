{# INTEGER type in some cases might mean days, microseconds, etc. #}
{# Refactor the last clause if it's the case. #}
{% macro get_latest_date(timestamp_column, timestamp_type) %}
  {%- if timestamp_type | upper == 'DATE' -%}
    (max({{ timestamp_column }}))
  {%- elif timestamp_type | upper == 'TIMESTAMP' -%}
    (extract(date from max({{ timestamp_column }})))
  {%- elif timestamp_type | upper == 'INTEGER' -%}
    (extract(date from timestamp_seconds(max({{ timestamp_column }}))))
  {% else %}
    {{ exceptions.raise_compiler_error('Invalid timestamp type. Expected: DATE, TIMESTAMP, INTEGER. Got: ' ~ timestamp_type) }}
  {%- endif -%}
{%- endmacro -%}
