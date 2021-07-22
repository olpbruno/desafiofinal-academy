with salesperson as (
    select * 
    from {{ ref('stg_salesperson')}}
),

person as (
    select * 
    from {{ ref('stg_person')}}
),

 final as (
    select 
        salesperson.*
        , person.nome 
        , person.tipo_pessoa
        , person.recebe_promo
    from salesperson
    left join person on salesperson.id_vendedor = person.id_pessoa
 )

select * from final