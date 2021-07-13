with staging as (
    select *
    from {{ ref('stg_city')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['id_endereco'])}} as city_sk -- chave surrogate hasheada
    from staging
)

select * from final