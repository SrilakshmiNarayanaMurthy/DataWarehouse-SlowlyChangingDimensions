-- depends_on: {{ ref('scd4_customers_current') }}

{{ config(
    materialized='incremental',
    unique_key='customer_id, archived_at_scd4'
) }}

with source as (
    select
        customer_id,
        customer_name,
        email,
        join_date,
        active
    from {{ source('customer_schema', 'customers_parsed') }}
)

{% if is_incremental() %}

, current_records as (
    select
        customer_id,
        customer_name,
        email,
        join_date,
        active
    from {{ ref('scd4_customers_current') }}
)

, changed_records as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.join_date,
        c.active,
        current_timestamp() as archived_at_scd4  -- ✅ the NEW unique column name
    from current_records c
    join source s
        on c.customer_id = s.customer_id
    where 
        c.customer_name != s.customer_name
        or c.email != s.email
        or c.join_date != s.join_date
        or c.active != s.active
)

select
    changed_records.customer_id,
    changed_records.customer_name,
    changed_records.email,
    changed_records.join_date,
    changed_records.active,
    changed_records.archived_at_scd4
from changed_records

{% else %}

-- First run: create schema but no data
select
    null as customer_id,
    null as customer_name,
    null as email,
    null as join_date,
    null as active,
    null as archived_at_scd4
where false

{% endif %}
