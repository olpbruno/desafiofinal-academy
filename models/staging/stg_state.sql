with source_data as (
    select 
        sd.stateprovinceid,
        sd.countryregioncode,
        sd.name
    from {{ source('adventure_works','stateprovince') }} as sd
)

select * 
from source_data