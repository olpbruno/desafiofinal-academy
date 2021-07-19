with source_data as (
    select 
        productmodelid as id_modelo
        , productdescriptionid as id_descricao
    from {{ source('adventure_works','productmodelproductdescriptionculture') }} 
    where cultureid = 'en'
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_modelo']) }} as sk_modelo
        , {{ dbt_utils.surrogate_key(['id_descricao']) }} as sk_descricao
    from source_data
)

select * 
from source_with_sk