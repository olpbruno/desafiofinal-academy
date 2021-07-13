with staging as (
    select *
    from {{ ref('stg_country')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['sigla_pais'])}} as country_sk -- chave surrogate hasheada
    from staging
)

select * from final