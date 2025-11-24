{{ config(materialized="table") }}

with limited_data as (
    select *
    from {{ source('tpcds', 'store_sales') }}
    limit 10
),
customer_subset as (
    select *
    from {{ source('tpcds', 'customer') }}
    limit 10
)

select
    c.C_CUSTOMER_SK as customer_id,
    sum(s.SS_SALES_PRICE) as total_sales,
    count(*) as order_count
from limited_data s
join customer_subset c
    on s.SS_CUSTOMER_SK = c.C_CUSTOMER_SK
group by 1
