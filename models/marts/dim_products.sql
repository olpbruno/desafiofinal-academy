with staging as (
    select * 
    from {{ ref('stg_product')}}
),

 final as (
    select *,
        {{dbt_utils.surrogate_key(['productid'])}} as product_sk -- chave surrogate hasheada
    from staging
 )

select * from final