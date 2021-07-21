with orders as (
    select *
    from {{ ref('stg_orderheader')}}
),

seed as (
    select *
    from {{ ref('seed_status')}}
),

orderdetail as (
    select * 
    from {{ ref('stg_orderdetail')}}
),

clients as (
    select * 
    from {{ ref('dim_clients')}}
),

salesperson as (
    select * 
    from {{ ref('dim_salesperson')}}
),

salesreason as (
    select * 
    from {{ ref('dim_salesreason')}}
),

products as (
    select * 
    from {{ ref('dim_products')}}
),

dates as (
    select 
        *
    from {{ ref('dim_dates')}}
),

final as (
    select 
        orders.sk_cliente
        , orderdetail.sk_produto
        , orderdetail.sk_pedido_item
        , salesperson.sk_vendedor
        , dates.sk_data
        , orders.sk_pedido
        , orders.id_pedido
        , orderdetail.id_item
        , case
            when salesreason.motivo is null
                then 'Not informed'
                else salesreason.motivo
            end as motivo
        , case
            when salesreason.tipo_motivo is null
                then 'Not informed'
                else salesreason.tipo_motivo
            end as tipo_motivo
        , orders.codigo_compra
        , orders.pedido_online
        , seed.status_pedido
        , orders.data_envio
        , orders.data_entrega
        , orders.data_pedido
        , orderdetail.quantidade
        , orderdetail.preco_unidade
        , orderdetail.desconto_unitario
        , orderdetail.subtotal
        , orders.frete
        , orders.impostos
        , orders.valor_total
    from orders
    left join orderdetail on orders.sk_pedido = orderdetail.sk_pedido
    left join clients on orders.sk_cliente = clients.sk_cliente
    left join products on orderdetail.sk_produto = products.sk_produto
    left join salesperson on orders.sk_vendedor = salesperson.sk_vendedor
    left join salesreason on orders.sk_pedido = salesreason.sk_pedido
    left join dates on orders.data_pedido = dates.full_date
    left join seed on orders.status_pedido = seed.id_status
)

select * from final