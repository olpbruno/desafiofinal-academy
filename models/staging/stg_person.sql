with source_data as (
    select 
        businessentityid as id_pessoa
        , case 
            when middlename is not null 
                then concat(firstname,' ', middlename, ' ', lastname) 
            else concat(firstname,' ', lastname) end as nome,
        persontype as tipo_pessoa			
        , case
            when emailpromotion = 1
                then 'Sim' 
                else 'Não'
            end as recebe_promo	
    from {{ source('adventure_works','person') }}
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_pessoa']) }} as sk_pessoa
    from source_data
)

select * 
from source_with_sk
