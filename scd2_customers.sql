{{ config(
    materialized='incremental',
    unique_key='customer_id, load_date'
) }}

with source as (
    select * from {{ source('customer_schema', 'customers_parsed') }}
)

{% if is_incremental() %}

, current_customers as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.join_date,
        c.active,
        c.load_date,
        c.current_flag
    from {{ this }} as c
    where c.current_flag = true
)

, new_customers as (
    select
        s.customer_id,
        s.customer_name,
        s.email,
        s.join_date,
        s.active,
        current_timestamp() as load_date,
        true as current_flag
    from source as s
    left join current_customers as c
        on s.customer_id = c.customer_id
    where c.customer_id is null
)

, updated_customers as (
    select
        s.customer_id,
        s.customer_name,
        s.email,
        s.join_date,
        s.active,
        current_timestamp() as load_date,
        true as current_flag
    from source as s
    join current_customers as c
        on s.customer_id = c.customer_id
    where s.customer_name != c.customer_name
       or s.email != c.email
       or s.join_date != c.join_date
       or s.active != c.active
)

-- THIS IS THE KEY FIX
, expired_customers as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.join_date,
        c.active,
        c.load_date,
        false as current_flag
    from current_customers as c
    where exists (
        select 1
        from updated_customers as u
        where c.customer_id = u.customer_id
    )
)

select * from new_customers
union all
select * from updated_customers
union all
select * from expired_customers

{% else %}

select
    customer_id,
    customer_name,
    email,
    join_date,
    active,
    current_timestamp() as load_date,
    true as current_flag
from source

{% endif %}
