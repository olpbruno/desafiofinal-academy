with staging as (
    select *
    from {{ ref('stg_workorderrouting')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['workorderid'])}} as workrouting_sk -- chave surrogate hasheada
    from staging
)

select * from final