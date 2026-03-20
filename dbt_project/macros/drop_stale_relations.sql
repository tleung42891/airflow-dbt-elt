{% macro drop_stale_relations(schema_name, database_name=target.database, dry_run=true) %}
    {% set mode = 'DRY_RUN' if dry_run else 'EXECUTE' %}
    {% set target = database_name ~ '.' ~ schema_name %}
    {{ log("[drop_stale_relations] start | mode=" ~ mode ~ " | target=" ~ target, info=True) }}

    {# Collect active dbt model relation names (log only after sort — one header + one line per model, no duplicate passes) #}
    {% set model_relation_names = [] %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model'
              and node.package_name == project_name
              and node.database == database_name
              and node.schema == schema_name %}
            {% set rel_name = node.alias if node.alias is not none else node.name %}
            {% do model_relation_names.append(rel_name | lower) %}
        {% endif %}
    {% endfor %}

    {% set sorted_models = model_relation_names | sort %}
    {{ log("[drop_stale_relations] Active dbt model relations in " ~ target ~ " (count=" ~ (sorted_models | length) ~ "):", info=True) }}
    {% for rel_name in sorted_models %}
        {{ log("  - " ~ rel_name, info=True) }}
    {% endfor %}

    {# List all existing table/view names in the database via a direct query (scoped to this schema, works on Postgres) #}
    {% set relations_sql %}
        select
            table_schema as schema,
            table_name   as name,
            table_type   as type
        from information_schema.tables
        where table_catalog = '{{ database_name }}'
          and table_schema = '{{ schema_name }}'
    {% endset %}

    {% set relations_result = run_query(relations_sql) %}

    {% if not execute or relations_result is none %}
        {{ log("[drop_stale_relations] abort | no query result or compile-only (execute=false)", info=True) }}
        {% do return([]) %}
    {% endif %}

    {% set existing_names = [] %}
    {% for row in relations_result %}
        {% do existing_names.append(row['name'] | lower) %}
    {% endfor %}

    {% set stale = [] %}
    {% for rel_name in existing_names %}
        {# Ignore raw_ tables (treated as ingestion sources) #}
        {% if (rel_name not in model_relation_names) and (rel_name[:4] != 'raw_') %}
            {% do stale.append(rel_name) %}
        {% endif %}
    {% endfor %}

    {% if stale | length == 0 %}
        {{ log("[drop_stale_relations] done | no_stale_relations | target=" ~ target, info=True) }}
        {% do return([]) %}
    {% endif %}

    {% set sorted_stale = stale | sort %}
    {{ log("[drop_stale_relations] Stale relations in " ~ target ~ " (count=" ~ (sorted_stale | length) ~ "):", info=True) }}
    {% for rel_name in sorted_stale %}
        {{ log("  - " ~ rel_name, info=True) }}
    {% endfor %}

    {% if dry_run %}
        {{ log("[drop_stale_relations] dry_run | no drops executed | set dry_run=false to drop", info=True) }}
        {% do return(stale) %}
    {% else %}
        {% set dropped = [] %}
        {% for rel_name in sorted_stale %}
            {{ log("[drop_stale_relations] drop | " ~ target ~ "." ~ rel_name, info=True) }}
            {% set drop_sql %}
                drop table if exists "{{ schema_name }}"."{{ rel_name }}" cascade;
                drop view  if exists "{{ schema_name }}"."{{ rel_name }}" cascade;
            {% endset %}
            {% do run_query(drop_sql) %}
            {% do dropped.append(rel_name) %}
        {% endfor %}
        {{ log("[drop_stale_relations] done | dropped_count=" ~ (dropped | length) ~ " | target=" ~ target, info=True) }}
        {% do return(dropped) %}
    {% endif %}
{% endmacro %}
