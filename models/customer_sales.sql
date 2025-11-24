{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with limited_customers as (
    select *
    from {{ source('tpcds', 'customer') }}
    limit 10   -- adjust this number to control how many customers to process per run
)

select
    c.C_CUSTOMER_SK as customer_id,
    sum(s.SS_SALES_PRICE) as total_sales,
    count(*) as order_count
from {{ source('tpcds', 'store_sales') }} s
join limited_customers c
    on s.SS_CUSTOMER_SK = c.C_CUSTOMER_SK
group by 1
