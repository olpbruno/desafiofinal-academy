with source_data as (
    select 
        row_number() over (order by countryregioncode) as countryregioncodeid,
        countryregioncode,
        name
    from {{ source('adventure_works','countryregion') }}
)

select * 
from source_data