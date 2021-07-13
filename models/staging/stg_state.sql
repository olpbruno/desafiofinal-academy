with source_data as (
    select 
        sd.stateprovinceid as id_estado,
        sd.name as estado
    from {{ source('adventure_works','stateprovince') }} as sd
)

select * 
from source_data