{{ config(
    materialized='incremental',
    unique_key='customer_id',
    schema='PUBLIC'  -- Ensures it goes to the PUBLIC schema
) }}

with customer_sales as (
    select
        c.C_CUSTOMER_SK as customer_id,
        c.C_FIRST_NAME as first_name,
        c.C_LAST_NAME as last_name,
        sum(s.SS_NET_PAID) as total_sales,
        count(*) as order_count
    from {{ source('tpcds', 'store_sales') }} s
    join {{ source('tpcds', 'customer') }} c
        on s.SS_CUSTOMER_SK = c.C_CUSTOMER_SK
    group by
        c.C_CUSTOMER_SK,
        c.C_FIRST_NAME,
        c.C_LAST_NAME
)

select *
from customer_sales
{% if is_incremental() %}
where customer_id not in (select customer_id from {{ this }})
{% endif %}
limit 10
