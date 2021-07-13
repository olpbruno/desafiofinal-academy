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

final as (
    select 
        orders.*
        , clients.client_sk
        , products.product_sk
        , salesreason.salesreason_sk
        , salesperson.vendedor_sk
        , city.city_sk
        , states.state_sk
        , country.country_sk
        , dates.date_sk
    from orders
)

select * from final