with 
    staging as (
        select * 
        from {{ ref('stg_country') }}
    )

select *
from staging