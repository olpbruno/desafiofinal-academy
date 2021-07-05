with source_data as (
    select 
        salesreasonid,
        name,
        reasontype
    from {{ source('adventure_works','salesreason') }}
)

select * 
from source_data