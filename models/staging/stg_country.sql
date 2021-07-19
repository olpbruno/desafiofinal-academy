with source_data as (
    select 
        countryregioncode as sigla_pais,
        name as pais,
        modifieddate
    from {{ source('adventure_works','countryregion') }}
),

deduplicated as (
        select
            *, ROW_NUMBER() over (
                partition by id_endereco 
                order by modifieddate DESC NULLS LAST) as index
        from source_data
    )

select * 
from deduplicated
where index = 1 