with staging as (
    select * 
    from {{ ref('stg_product')}}
),

 final as (
    select *,
        {{dbt_utils.surrogate_key(['id_produto'])}} as product_sk -- chave surrogate hasheada
    from staging
 )

select * from final