{% macro write_to_parquet(source_table, destination_path) -%}
    {%- set copy_query = "copy " + source_table + " TO '" 
        + destination_path + "' WITH (FORMAT csv, HEADER true)" -%}

    {% do run_query(copy_query) %}
{%- endmacro %}