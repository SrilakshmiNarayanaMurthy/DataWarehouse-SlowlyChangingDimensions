{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with source as (
    select * from {{ source('customer_schema', 'customers_parsed') }}
)

select
    customer_id,
    customer_name,
    email,
    join_date,
    active,
    current_timestamp() as load_date
from source
