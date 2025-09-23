{% test stg_tpch_nation_soft_dlt_mismatch(model, source_table, src_id_col, tgt_id_col,delete_flag_col) %}

with source_data as (
    select {{ src_id_col }} as id
    from {{ source_table }}
),
target_data as (
    select {{ tgt_id_col }} as id, {{ delete_flag_col }} as is_deleted
    from {{ model }}
)
-- 1. False positives → row marked deleted but still exists in source
select
    t.id,
    'false_positive_soft_delete' as issue_type
from target_data t
where t.is_deleted = true
  and exists (select 1 from source_data s where s.id = t.id)

union all

-- 2. False negatives → row not deleted in target but missing from source
select
    t.id,
    'false_negative_soft_delete' as issue_type
from target_data t
where t.is_deleted = false
  and not exists (select 1 from source_data s where s.id = t.id)

{% endtest %}
