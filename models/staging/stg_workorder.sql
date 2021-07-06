with source_data as (
    select *
    from {{ source('adventure_works','stateprovince') }} as sd
)

select * 
from source_data