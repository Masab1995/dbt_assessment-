{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with next_batch as (
    select *
    from {{ source('tpcds', 'customer') }}
    where C_CUSTOMER_SK not in (select customer_id from {{ this }})
    order by C_CUSTOMER_SK
    limit 10
),

sales_data as (
    select *
    from {{ source('tpcds', 'store_sales') }}
)

select
    c.C_CUSTOMER_SK as customer_id,
    sum(s.SS_SALES_PRICE) as total_sales,
    count(*) as order_count
from next_batch c
join sales_data s
    on c.C_CUSTOMER_SK = s.SS_CUSTOMER_SK
group by 1
