with source_data as (
    select 
        businessentityid as id_vendedor
        , territoryid as id_territorio
        , salesquota as previsao_vendas
    from {{ source('adventure_works','salesperson') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_vendedor']) }} as sk_vendedor
        , {{ dbt_utils.surrogate_key(['id_territorio']) }} as sk_territorio
    from source_data
)

select * 
from source_with_sk