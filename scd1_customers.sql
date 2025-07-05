{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

{% if is_incremental() %}

with source as (
    select * from {{ source('customer_schema', 'customers_parsed') }}
),
existing as (
    select * from {{ this }}
),
final as (
    select
        s.customer_id,
        s.customer_name,
        s.email,
        s.join_date,
        s.active
    from source s
    left join existing e on s.customer_id = e.customer_id
    where e.customer_id is null 
       or md5(s.customer_name || s.email || s.join_date || s.active) 
            != md5(e.customer_name || e.email || e.join_date || e.active)
)

select * from final

{% else %}

select
    customer_id,
    customer_name,
    email,
    join_date,
    active
from {{ source('customer_schema', 'customers_parsed') }}

{% endif %}
