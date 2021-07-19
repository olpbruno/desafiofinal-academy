with source_data as (
    select 
        productsubcategoryid as id_subcategoria
        , productcategoryid as id_categoria
        , name as subcategoria
    from {{ source('adventure_works','productsubcategory') }} 
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_subcategoria']) }} as sk_subcategoria        
        , {{ dbt_utils.surrogate_key(['id_categoria']) }} as sk_categoria
    from source_data
)

select * 
from source_with_sk