{#
    This macro controls how dbt decides the final schema name in Snowflake.

    In dbt, a "schema" is like a folder/group inside a database.
    Use clean schema names like:

        STAGING
        INTERMEDIATE
        MARTS

    Without this macro, dbt may combine names and create schemas like:

        STAGING_INTERMEDIATE

    This macro prevents that and tells dbt:
    "Use the schema name exactly as I write it in dbt_project.yml."
#}


{# 
    This defines a dbt macro called generate_schema_name.

    dbt automatically looks for this macro name.
    When dbt builds a model, it calls this macro to decide
    which schema the model should be created in.

    custom_schema_name = the schema you written in dbt_project.yml
    node = the dbt model/test/seed/etc. currently being processed

    Do not use "node" in this macro, but dbt still expects it
    to be included as an argument.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {#
        This checks if the model does NOT have a custom schema.

        Example:
        If a model does not say "+schema: staging"
        then custom_schema_name will be none.

        In that case, dbt should use the default target schema
        from profiles.yml file.
    #}
    {%- if custom_schema_name is none -%}

        {#
            target.schema comes from dbt profile.

            Example:
            If profiles.yml says schema: PUBLIC,
            then dbt will use PUBLIC.
        #}
        {{ target.schema }}

    {#
        This runs when the model DOES have a custom schema.

        Example:
        If dbt_project.yml says:
            +schema: STAGING

        Then custom_schema_name is STAGING.
    #}
    {%- else -%}

        {#
            This tells dbt to use the custom schema exactly as written.

            The "| trim" removes extra spaces before or after the name.

            Example:
            " STAGING " becomes "STAGING"
        #}
        {{ custom_schema_name | trim }}

    {#
        This closes the if/else condition.
    #}
    {%- endif -%}

{#
    This closes the macro.
#}
{%- endmacro %}