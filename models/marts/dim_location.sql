with staging as (
    select *
    from {{ ref('stg_location')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['locationid'])}} as location_sk -- chave surrogate hasheada
    from staging
)

select * from final