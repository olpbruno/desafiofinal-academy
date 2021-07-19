with source_data as (
    select 
        salesreasonid as id_razaovenda,
        name as motivo,
        reasontype as tipo_motivo
    from {{ source('adventure_works','salesreason') }}
)

select * 
from source_data