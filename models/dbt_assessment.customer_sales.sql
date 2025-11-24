{{ config(materialized="table") }}

select
    c.customer_id,
    sum(s.sales_price) as total_sales,
    count(*) as order_count
from {{ source('tpcds', 'store_sales') }} s
join {{ source('tpcds', 'customer') }} c
    on s.customer_id = c.customer_id
group by 1
