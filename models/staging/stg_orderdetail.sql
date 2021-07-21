with source_data as (
    select 
        salesorderdetailid as id_item
        , salesorderid as id_pedido
        , productid as id_produto
        , orderqty as quantidade
        , unitprice as preco_unidade
        , unitpricediscount as desconto_unitario
        , (unitprice*(1 - unitpricediscount)*orderqty) as subtotal
    from {{ source('adventure_works','salesorderdetail') }}
),

source_with_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_item']) }} as sk_item
        , {{ dbt_utils.surrogate_key(['id_pedido']) }} as sk_pedido
        , {{ dbt_utils.surrogate_key(['id_produto']) }} as sk_produto
        , {{ dbt_utils.surrogate_key(['id_item','id_pedido']) }} as sk_pedido_item
    from source_data
)

select *
from source_with_sk