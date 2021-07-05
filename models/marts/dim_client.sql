with staging as (
    select * 
    from {{ ref('stg_person')}}
    where persontype in ('SC', 'IN')
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['businessentityid'])}} as client_sk -- chave surrogate hasheada
    from staging
)

select * from final