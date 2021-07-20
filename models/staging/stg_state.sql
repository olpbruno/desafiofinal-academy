with source_data as (
    select 
        stateprovinceid as id_estado
        , name as estado	
        , countryregioncode as sigla_pais
    from {{ source('adventure_works','stateprovince') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_estado']) }} as sk_estado
        , {{ dbt_utils.surrogate_key(['sigla_pais']) }} as sk_pais
    from source_data
)

select * 
from source_with_sk