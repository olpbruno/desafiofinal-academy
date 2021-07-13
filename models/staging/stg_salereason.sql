with source_data as (
    select 
        salesreasonid,
        name as sale_reason,
        reasontype
    from {{ source('adventure_works','salesreason') }}
)

select * 
from source_data