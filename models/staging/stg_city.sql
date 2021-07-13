with source_data as (
    select 
        addressid as id_endereco,
        city as cidade,
        stateprovinceid as id_estado
    from {{ source('adventure_works','address') }}
)

select * 
from source_data