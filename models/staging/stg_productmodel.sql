with source_data as (
    select 
        productmodelid as id_modelo
        , name as modelo
    from {{ source('adventure_works','productmodel') }} 
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_modelo']) }} as sk_modelo
    from source_data
)

select * 
from source_with_sk