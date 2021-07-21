with source_data as (
    select 
    salesorderid as id_pedido
    , salesreasonid as id_motivo
    , modifieddate
    from {{ source('adventure_works','salesorderheadersalesreason') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_pedido']) }} as sk_pedido
        , {{ dbt_utils.surrogate_key(['id_motivo']) }} as sk_motivo
    from source_data
),

deduplicated as(
    select 
        *
        , row_number () over(
            partition by sk_pedido
            order by modifieddate desc) as index
    from source_with_sk
)

select * 
from deduplicated
where index = 1