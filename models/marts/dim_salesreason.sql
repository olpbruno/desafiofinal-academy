with staging as (
    select *
    from {{ ref('stg_salereason')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['id_razaovenda'])}} as salesreason_sk -- chave surrogate hasheada
    from staging
)

select * from final