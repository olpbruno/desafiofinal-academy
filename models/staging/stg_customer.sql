with source_data as (
    select 
        personid as id_pessoa,						
        storeid as id_loja,		
        customerid as id_cliente
    from {{ source('adventure_works','customer') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_pessoa']) }} as sk_pessoa
        , {{ dbt_utils.surrogate_key(['id_cliente']) }} as sk_cliente
        , {{ dbt_utils.surrogate_key(['id_loja']) }} as sk_loja
    from source_data
)

select * 
from source_with_sk