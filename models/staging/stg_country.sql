with source_data as (
    select 
        name as pais,
        countryregioncode as sigla_pais
    from {{ source('adventure_works','countryregion') }}
),


source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['sigla_pais']) }} as sk_pais
    from source_data
)

select * 
from source_with_sk