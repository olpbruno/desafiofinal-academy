with orders as (
    select *
    from {{ ref('stg_orderheader')}}
),

seed as (
    select *
    from {{ ref('seed_status')}}
),

clients as (
    select * 
    from {{ ref('dim_clients')}}
),

city as (
    select * 
    from {{ ref('dim_city')}}
),

country as (
    select * 
    from {{ ref('dim_country')}}
),

states as (
    select * 
    from {{ ref('dim_state')}}
),

salesperson as (
    select * 
    from {{ ref('dim_salesperson')}}
),

salesreason as (
    select * 
    from {{ ref('dim_salesreason')}}
),

dates as (
    select 
        *
    from {{ ref('dim_dates')}}
),

final as (
    select 
        orders.sk_cliente
        , orders.sk_endereco
        , country.sk_pais
        , states.sk_estado
        , orders.sk_vendedor
        , dates.sk_data
        , orders.sk_pedido
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
        , seed.status_pedido
        , orders.data_envio
        , orders.data_entrega
        , orders.data_pedido
        , orders.frete
        , orders.impostos
        , orders.valor_total
    from orders
    left join clients on orders.sk_cliente = clients.sk_cliente
    left join salesperson on orders.sk_vendedor = salesperson.sk_vendedor
    left join salesreason on orders.sk_pedido = salesreason.sk_pedido
    left join dates on orders.data_pedido = dates.dates
    left join seed on orders.status_pedido = seed.id_status
    left join city on orders.sk_endereco = city.sk_endereco
    left join states on city.sk_estado = states.sk_estado
    left join country on states.sk_pais = country.sk_pais
)

select * 
from final