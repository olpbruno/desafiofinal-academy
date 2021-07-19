with source_data as (
    select
        productdescriptionid as id_descricao,
        description as descricao
    from {{ source('adventure_works','productdescription') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_descricao']) }} as sk_descricao
    from source_data
)

select * 
from source_with_sk