with source_data as (
    select 
        addressid as id_endereco
        , city as cidade
        , stateprovinceid as id_estado
        , postalcode as codigo_postal
    from {{ source('adventure_works','address') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_estado']) }} as sk_estado
        , {{ dbt_utils.surrogate_key(['id_endereco']) }} as sk_endereco
    from source_data
)

select * 
from source_with_sk