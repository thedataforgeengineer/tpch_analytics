--orders_growth_analysis.sql
--purpose: see how orders have trended month-over-month in the year 1998

select date_trunc('month',order_date) as order_month,count(1)
from {{ref("stg_tphc_orders")}}
where order_date>='1998-01-01'
group by order_month
order by 1 asc 