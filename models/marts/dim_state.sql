with staging as (
    select *
    from {{ ref('stg_state')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['stateprovinceid'])}} as state_sk -- chave surrogate hasheada
    from staging
)

select * from final