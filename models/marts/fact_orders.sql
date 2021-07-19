with orders as (
    select *
    from {{ ref('stg_orders')}}
),

clients as (
    select * 
    from {{ ref('dim_clients')}}
),

country as (
    select * 
    from {{ ref('dim_country')}}
),

states as (
    select * 
    from {{ ref('dim_states')}}
),

city as (
    select * 
    from {{ ref('dim_city')}}
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
        date_sk
        , cast(full_date as timestamp) as full_date
    from {{ ref('dim_dates')}}
),

final as (
    select 
        clients.client_sk
        , products.sk_produto
        , salesreason.salesreason_sk
        , salesperson.vendedor_sk
        , city.city_sk
        , states.state_sk
        , country.country_sk
        , dates.date_sk
        , orders.order_sk
        , orders.id_pedido
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
        , orders.situacao_pedido
        , orders.data_envio
        , orders.data_entrega
        , orders.quantidade
        , orders.preco_unidade
        , orders.desconto_unitario
        , orders.subtotal
        , orders.frete
        , orders.impostos
        , orders.valor_total
    from orders
    left join clients on orders.id_pessoa = clients.id_pessoa
    left join products on orders.id_produto = products.id_produto
    left join salesperson on orders.id_vendedor = salesperson.id_pessoa
    left join salesreason on orders.id_razaovenda = salesreason.id_razaovenda
    left join city on orders.id_endereco = city.id_endereco
    left join states on orders.id_estado = states.id_estado
    left join country on orders.sigla_pais = country.sigla_pais
    left join dates on orders.data_pedido = dates.full_date
)

select * from final