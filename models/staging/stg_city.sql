with source_data as (
    select 
        addressid as id_endereco,
        city as cidade,
        stateprovinceid as id_estado,
        modifieddate
    from {{ source('adventure_works','address') }}
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