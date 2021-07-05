with staging as (
    select * 
    from {{ ref('stg_person')}}
    where persontype = 'SP'
),

 final as (
    select *,
        {{dbt_utils.surrogate_key(['businessentityid'])}} as vendedor_sk -- chave surrogate hasheada
    from staging
 )

select * from final