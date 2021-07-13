with source_data as (
    select 
        salesreasonid as id_razaovenda,
        name as razao_venda,
        reasontype as tipo_razao
    from {{ source('adventure_works','salesreason') }}
)

select * 
from source_data