with source_data as (
    select 
        businessentityid as id_entidade,						
        addressid as id_endereco
    from {{ source('adventure_works','businessentityaddress') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_entidade']) }} as sk_entidade
        , {{ dbt_utils.surrogate_key(['id_endereco']) }} as sk_endereco
    from source_data
)

select * 
from source_with_sk