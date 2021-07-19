with source_data as (
    select 
        productcategoryid as id_categoria
        , name as categoria
    from {{ source('adventure_works','productcategory') }} 
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_categoria']) }} as sk_categoria
    from source_data
)

select * 
from source_with_sk