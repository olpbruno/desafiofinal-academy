with source_data as (
    select 
        salesreasonid as id_motivo,
        name as motivo,
        reasontype as tipo_motivo,
        modifieddate
    from {{ source('adventure_works','salesreason') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_motivo']) }} as sk_motivo
    from source_data
),

deduplicated as(
    select 
        *
        , row_number () over(
            partition by sk_motivo
            order by modifieddate desc) as index
    from source_with_sk
)

select * 
from deduplicated
where index = 1