with source_data as (
    select 
    locationid
    , scheduledstartdate
    , actualcost
    , plannedcost
    , modifieddate
    , actualenddate
    , scheduledenddate
    , actualresourcehrs
    , productid
    , actualstartdate
    , workorderid
    , operationsequence
    from {{ source('adventure_works','workorderrouting') }}
),

deduplicated as (
        select
            *, ROW_NUMBER() over (
                partition by workorderid 
                order by modifieddate DESC NULLS LAST) as index
        from source_data
    )

select *
from deduplicated
where index = 1