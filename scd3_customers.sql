{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with source as (
    select * from {{ source('customer_schema', 'customers_parsed') }}
)

{% if is_incremental() %}

, current_records as (
    select * from {{ this }}
)

, merged as (
    select
        s.customer_id,
        s.customer_name,
        s.email,
        s.join_date,
        s.active,
        case
            when s.email != c.email then c.email
            else c.prev_email
        end as prev_email
    from source s
    left join current_records c
        on s.customer_id = c.customer_id
)

select * from merged

{% else %}

-- First run: prev_email is null
select
    customer_id,
    customer_name,
    email,
    join_date,
    active,
    null as prev_email
from source

{% endif %}
