with source_workorder as (
    select *
    from {{ source('adventure_works','workorder') }}
),

source_scrapreason as (
    select *
    from {{ source('adventure_works','scrapreason') }}
),

final as (
    select 
        source_workorder.*
        , source_scrapreason.name as scrapreason
    from source_workorder
    left join source_scrapreason on source_workorder.scrapreasonid = source_scrapreason.scrapreasonid
)

select * 
from final