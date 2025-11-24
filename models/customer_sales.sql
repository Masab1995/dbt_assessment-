{{ config(materialized="table") }}

select
    c.C_CUSTOMER_SK as customer_id,
    sum(s.SS_SALES_PRICE) as total_sales,
    count(*) as order_count
from {{ source('tpcds', 'store_sales') }} s
join {{ source('tpcds', 'customer') }} c
    on s.SS_CUSTOMER_SK = c.C_CUSTOMER_SK
group by 1
