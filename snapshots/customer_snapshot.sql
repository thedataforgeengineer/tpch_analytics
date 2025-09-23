{% snapshot customer_snapshot %}
    {{
        config(
            target_schema="snapshots",
            unique_key="cust_key",
            strategy="timestamp",
            updated_at="updated_at",
        )
    }}
    select
        c_custkey as cust_key,
        c_name as name,
        c_address as address,
        c_nationkey as nation_key,
        c_phone as phone,
        c_acctbal as accnt_bal,
        c_mktsegment as mkt_segment,
        c_comment as comment,
        updated_at,
        load_type
    from {{ source("raw_inc_test", "customer") }}
{% endsnapshot %}
