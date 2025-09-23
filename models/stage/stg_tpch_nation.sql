{{
    config(
        materialized="incremental",
        unique_key="nation_key",
        incremental_strategy="merge",
    )
}}

with
    raw_nation as (select * from {{ source("raw_inc_test", "nation")}}
    qualify row_number() over(partition by n_nationkey order by updated_at desc)=1
    ),
    -- Full target (historical state)
    target as (select * from {{ this }}),
    -- New or updated rows
    new_or_updated as (
        select
            n_nationkey as nation_key,
            n_nationname as nation_name,
            n_regionkey as region_key,
            n_comment as comment,
            false is_deleted,
            updated_at,
            load_type
        from raw_nation
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    -- Soft delete: rows missing in source
    soft_deleted as (
        select
            t.nation_key,
            t.nation_name,
            t.region_key,
            t.comment,
            true as is_deleted,
            current_timestamp::timestamp_ntz(9) as updated_at,  -- deletion time
            t.load_type
        from target t
        left join raw_nation s on t.nation_key = s.n_nationkey
        where s.n_nationkey is null and t.is_deleted = false
    )

select *
from new_or_updated
union all
select *
from soft_deleted
