with salesreason as (
    select *
    from {{ ref('stg_salereason')}}
),

salesorderheadersalesreason as (
    select *
    from {{ ref('stg_salesorderheadersalesreason')}}
),

final as (
    select 
        salesorderheadersalesreason.sk_pedido
        , salesreason.sk_motivo
        , salesreason.motivo
        , salesreason.tipo_motivo
    from salesorderheadersalesreason
    left join salesreason
        on salesorderheadersalesreason.sk_motivo = salesreason.sk_motivo
)

select * from final