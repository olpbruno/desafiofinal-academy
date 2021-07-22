with 
    staging as (
        select * 
        from {{ ref('stg_address') }}
    )

select *
from staging