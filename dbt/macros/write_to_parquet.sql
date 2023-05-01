{% macro write_to_parquet(source_table, destination_path) -%}
    {%- set copy_query = "copy " + source_table + " TO '" 
        + destination_path + "' WITH (FORMAT parquet, CODEC 'ZSTD', ROW_GROUP_SIZE 100000)" -%}

    {% do run_query(copy_query) %}
{%- endmacro %}