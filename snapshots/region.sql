{% snapshot region %}
    {{
        config(
            target_schema="snapshots",
            unique_key="region_key",
            strategy="check",
            check_cols=['name','comment']
        )
    }}
    select
        r_regionkey as region_key,
        r_name as name,
        r_comment as comment,
        load_type
    from {{ source("raw_inc_test", "region") }}
{% endsnapshot %}
