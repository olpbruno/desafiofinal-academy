with source_data as (
    select *
    from {{ source('adventure_works','location') }}
)

select * 
from source_data