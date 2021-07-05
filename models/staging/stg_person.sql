with source_customer as (
    select 
        personid,						
        storeid,		
        customerid	
    from {{ source('adventure_works','customer') }}
),

source_businessentityaddress as (
    select 
        businessentityid,						
        addressid	
    from {{ source('adventure_works','businessentityaddress') }}
),

source_address as (
    select 
        addressid,
        city,						
        stateprovinceid
    from {{ source('adventure_works','address') }}
),

source_stateprovince as (
    select 
        stateprovinceid,						
        name as state_province,	
        countryregioncode,	
        territoryid	
    from {{ source('adventure_works','stateprovince') }}
),

source_territory as (
    select 
        tr.territoryid,
        tr.name as territory_name,
        tr.group as grupo	
    from {{ source('adventure_works','salesterritory') }} as tr
),


source_countryregion as (
    select 
        cr.name as country_region,
        cr.countryregioncode
    from {{ source('adventure_works','countryregion') }} as cr
),

source_person as (
    select 
        businessentityid,
        case 
            when middlename is not null 
                then concat(firstname,' ', middlename, ' ', lastname) 
            else concat(firstname,' ', lastname) end as clientname,
        persontype,	
        namestyle,	
        suffix,	
        modifieddate,		
        emailpromotion,	
        title,
        rowguid
    from {{ source('adventure_works','person') }}
),

final as (
    select 
        source_person.businessentityid,
        clientname as name,
        persontype,	
        namestyle,	
        suffix,	
        modifieddate,
        rowguid,	
        emailpromotion,	
        title,        
        grupo,	
        territory_name,
        country_region,
        state_province,
        city
        from source_person
        left join source_customer on source_person.businessentityid = source_customer.personid
        left join source_businessentityaddress on source_person.businessentityid = source_businessentityaddress.businessentityid
        left join source_address on source_businessentityaddress.addressid = source_address.addressid
        left join source_stateprovince on source_address.stateprovinceid = source_stateprovince.stateprovinceid
        left join source_countryregion on source_stateprovince.countryregioncode = source_countryregion.countryregioncode
        left join source_territory on source_stateprovince.territoryid = source_territory.territoryid
)

select *
from final