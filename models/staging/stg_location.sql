with source_data as (
    select 
        costrate as taxa_custo
        , locationid
        , availability as capacidade
        , modifieddate
        , name as localizacao
    from {{ source('adventure_works','location') }}
),

deduplicated as (
        select
            *, ROW_NUMBER() over (
                partition by locationid 
                order by modifieddate DESC NULLS LAST) as index
        from source_data
    )

select *
from deduplicated
where index = 1
