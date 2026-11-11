{# Unix epoch seconds (float) -> timestamp, on BigQuery and DuckDB. #}
{% macro epoch_to_ts(col) %}{{ return(adapter.dispatch('epoch_to_ts')(col)) }}{% endmacro %}

{% macro default__epoch_to_ts(col) %}to_timestamp({{ col }}){% endmacro %}

{% macro bigquery__epoch_to_ts(col) %}timestamp_micros(cast({{ col }} * 1000000 as int64)){% endmacro %}
