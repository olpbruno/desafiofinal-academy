with source_data as (
    select 
        salesorderid as id_pedido
        , revisionnumber as numero_revisao
        , orderdate as data_pedido
        , shipdate as data_envio
        , duedate as data_entrega
        , status as status_pedido
        , case 
            when onlineorderflag is true
                then 'Sim'
                else 'Não'
            end as pedido_online
        , purchaseordernumber as codigo_compra
        , accountnumber as numero_conta
        , customerid as id_cliente
        , salespersonid id_vendedor
        , taxamt as impostos
        , freight as frete
        , totaldue as valor_total
    from {{ source('adventure_works','salesorderheader') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_pedido']) }} as sk_pedido
        , {{ dbt_utils.surrogate_key(['id_cliente']) }} as sk_cliente
        , {{ dbt_utils.surrogate_key(['id_vendedor']) }} as sk_vendedor
        , {{ dbt_utils.surrogate_key(['data_entrega']) }} as sk_data   
    from source_data
)

select *
from source_with_sk
