{# Does `col` match the regex `pattern`? BigQuery and DuckDB spell it differently. #}
{% macro regex_match(col, pattern) %}{{ return(adapter.dispatch('regex_match')(col, pattern)) }}{% endmacro %}

{% macro default__regex_match(col, pattern) %}regexp_matches({{ col }}, '{{ pattern }}'){% endmacro %}

{% macro bigquery__regex_match(col, pattern) %}regexp_contains({{ col }}, r'{{ pattern }}'){% endmacro %}
