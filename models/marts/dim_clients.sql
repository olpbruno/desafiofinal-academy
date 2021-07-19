with staging as (
    select * 
    from {{ ref('stg_person')}}
),

final as (
    select *,
        {{dbt_utils.surrogate_key(['id_pessoa'])}} as client_sk -- chave surrogate hasheada
    from staging
)

select *
from final
