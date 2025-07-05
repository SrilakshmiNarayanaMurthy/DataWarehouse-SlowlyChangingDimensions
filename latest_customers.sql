select *
from {{ ref('scd2_customers') }}
where current_flag = true
