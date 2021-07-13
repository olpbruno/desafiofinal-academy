with source_orderheader as (
    select 
        salesorderid as id_pedido
        , revisionnumber as numero_revisao
        , orderdate as data_pedido
        , shipdate as data_envio
        , duedate as data_entrega
        , case
            when status = 1
                then 'Em processo'
            when status = 2
                then 'Aprovado'
            when status = 3
                then 'Em espera'
            when status = 4
                then 'Rejeitado'
            when status = 5
                then 'Enviado'
                else 'Cancelado'
        end as situacao_pedido
        , case 
            when onlineorderflag is true
                then 'Sim'
                else 'Não'
            end as pedido_online
        , purchaseordernumber as codigo_compra
        , accountnumber as numero_conta
        , customerid as id_cliente
        , salespersonid id_vendedor
        , shiptoaddressid as id_endereco_entrega
        , taxamt as impostos
        , freight as frete
        , totaldue as valor_total
        , {{dbt_utils.surrogate_key(['salesorderid'])}} as order_sk
    from {{ source('adventure_works','salesorderheader') }}
),

source_address as (
    select 
        addressid,
        stateprovinceid
    from {{ source('adventure_works','address') }}
),

source_stateprovince as (
    select 
        stateprovinceid,
        countryregioncode
    from {{ source('adventure_works','stateprovince') }}
),

source_orderdetail as (
    select 
        salesorderid
        , salesorderdetailid
        , carriertrackingnumber
        , orderqty
        , productid
        , specialofferid
        , unitprice
        , unitpricediscount
        , (unitprice*(1 - unitpricediscount)*orderqty) as subtotal
        , {{dbt_utils.surrogate_key(['salesorderdetailid'])}} as orderdetail_sk
    from {{ source('adventure_works','salesorderdetail') }}
),

source_salesorderheadersalesreason as (
    select *
    from {{ source('adventure_works','salesorderheadersalesreason') }}
),

source_salesreason as (
    select *
    from {{ source('adventure_works','salesreason') }}
),

orders as (
    select 
        source_orderheader.*
        , source_orderdetail.orderdetail_sk
        , source_orderdetail.orderqty as quantidade
        , source_orderdetail.unitprice as preco_unidade
        , source_orderdetail.unitpricediscount as desconto_unitario
        , source_orderdetail.subtotal
        , source_orderdetail.productid as id_produto
        , source_salesreason.salesreasonid as id_razaovenda
        , source_address.addressid as id_endereco
        , source_address.stateprovinceid as id_estado
        , source_stateprovince.countryregioncode as sigla_pais
    from source_orderheader
    left join source_orderdetail on source_orderheader.id_pedido = source_orderdetail.salesorderid
    left join source_salesorderheadersalesreason on source_orderheader.id_pedido = source_salesorderheadersalesreason.salesorderid
    left join source_salesreason on source_salesorderheadersalesreason.salesreasonid = source_salesreason.salesreasonid
    left join source_address on source_orderheader.id_endereco_entrega = source_address.addressid
    left join source_stateprovince on source_address.stateprovinceid = source_stateprovince.stateprovinceid
)

select * from orders