with source_data as (
    select *
    from {{ source('adventure_works','workorderrouting') }}
)

select * 
from source_data