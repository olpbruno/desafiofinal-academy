with source_data as (
    select 
        name as loja,
        salespersonid as id_vendedor,
        businessentityid as id_loja
    from {{ source('adventure_works','store') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_loja']) }} as sk_loja
        , {{ dbt_utils.surrogate_key(['id_vendedor']) }} as sk_vendedor
    from source_data
)

select * 
from source_with_sk