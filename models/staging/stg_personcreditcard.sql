with source_data as (
    select 
        creditcardid as id_cartao,
        businessentityid as id_pessoa
    from {{ source('adventure_works','personcreditcard') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_cartao']) }} as sk_cartao
        , {{ dbt_utils.surrogate_key(['id_pessoa']) }} as sk_pessoa
    from source_data
)

select * 
from source_with_sk