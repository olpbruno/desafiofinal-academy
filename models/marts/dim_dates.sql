with staging as (
    select *
    from {{ ref('stg_dates')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['id'])}} as date_sk -- chave surrogate hasheada
    from staging
)

select * from final