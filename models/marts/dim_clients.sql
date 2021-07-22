with customer as (
    select * 
    from {{ ref('stg_customer')}}
),

address as (
    select * 
    from {{ ref('stg_address')}}
),

businessentityaddress as (
    select * 
    from {{ ref('stg_businessentityaddress')}}
),

country as (
    select * 
    from {{ ref('stg_country')}}
),

state as (
    select * 
    from {{ ref('stg_state')}}
),

person as (
    select *
    from {{ ref('stg_person')}}

),

personcreditcard as (
    select * 
    from {{ ref('stg_personcreditcard')}}
),

creditcard as (
    select * 
    from {{ ref('stg_creditcard')}}
),

store as (
    select * 
    from {{ ref('stg_store')}}
),

stores as (
    select 
        customer.id_cliente	
        , customer.sk_cliente
        , person.nome  
        , 'SC' as tipo_pessoa /* SC = Store contact */
        , 'Not informed' as tipo_cartao
        , 'Não' as recebe_promo
    from customer
    left join store on customer.sk_loja = store.sk_loja
    left join person on store.sk_vendedor = person.sk_pessoa
    left join businessentityaddress on store.sk_loja = businessentityaddress.sk_entidade
    left join address on businessentityaddress.sk_endereco = address.sk_endereco
    left join state on address.sk_estado = state.sk_estado
    left join country on state.sk_pais = country.sk_pais
    where customer.id_pessoa is null
),

retail_customer as (
    select 
        customer.id_cliente	
        , customer.sk_cliente
        , person.nome as nome
        , person.tipo_pessoa
        , cast(coalesce(creditcard.tipo_cartao,'Not informed') as string)
        , person.recebe_promo
    from customer
    left join person on customer.sk_pessoa = person.sk_pessoa
    left join businessentityaddress on person.sk_pessoa = businessentityaddress.sk_entidade
    left join address on businessentityaddress.sk_endereco = address.sk_endereco
    left join state on address.sk_estado = state.sk_estado
    left join country on state.sk_pais = country.sk_pais
    left join personcreditcard on customer.sk_pessoa = personcreditcard.sk_pessoa
    left join creditcard on personcreditcard.sk_cartao = creditcard.sk_cartao
    where customer.id_pessoa is not null
),
    /* União entre as tabelas stores e retail_customer */
tabela_final as (
    select *
    from stores
    union all
    select *
    from retail_customer
)
select *
from tabela_final
