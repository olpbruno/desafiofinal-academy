with source_data as (
    select 
        creditcardid as id_cartao
        , cardtype as tipo_cartao
        , cardnumber as numero_cartao
        , concat(expyear,'-', expmonth) as validade_cartao
    from {{ source('adventure_works','creditcard') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_cartao']) }} as sk_cartao
    from source_data
)

select * 
from source_with_sk