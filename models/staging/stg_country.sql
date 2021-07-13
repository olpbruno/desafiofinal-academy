with source_data as (
    select 
        countryregioncode as sigla_pais,
        name as pais
    from {{ source('adventure_works','countryregion') }}
)

select * 
from source_data