with 
    staging as (
        select * 
        from {{ ref('stg_state') }}
    )

select *
from staging