-- The purpose of this model is to source raw data from the orders table (defined as a source in dbt) and standardize the column names for easier readability and consistency.
{{ config(materialized='table') }} -- creates the model as a table
with source as ( 

    select * from {{ source('raw_tpch', 'orders') }} -- dbt understands order is a source table under the raw_tpch source

),

renamed as (

    select

        o_orderkey as order_key,
        o_custkey as customer_key,
        o_orderstatus as status_code,
        o_totalprice as total_price,
        o_orderdate as order_date,
        o_orderpriority as priority_code,
        o_clerk as clerk_name,
        o_shippriority as ship_priority,
        o_comment as comment

    from source

)
select * from renamed