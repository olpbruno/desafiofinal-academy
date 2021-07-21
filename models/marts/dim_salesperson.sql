with address as (
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

tabela_final as (
    select
        person.sk_pessoa as sk_vendedor
        , person.nome as nome
        , person.recebe_promo
        , address.cidade
        , address.codigo_postal
        , state.estado
        , country.pais
    from person
    left join businessentityaddress on person.sk_pessoa = businessentityaddress.sk_entidade
    left join address on businessentityaddress.sk_endereco = address.sk_endereco
    left join state on address.sk_estado = state.sk_estado
    left join country on state.sk_pais = country.sk_pais
    where person.tipo_pessoa = 'SP'
)

select *
from tabela_final