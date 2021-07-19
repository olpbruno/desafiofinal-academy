with workrouting as (
    select *
    from {{ ref('dim_workrouting')}}
),

locations as (
    select *
    from {{ ref('dim_location')}}
),

products as (
    select *
    from {{ ref('dim_products')}}
),

workorder as (
    select 
        workorderid
        , startdate 
        , scrapreasonid
        , scrappedqty
        , productid
        , orderqty
        , enddate
        , duedate
        , case
            when scrapreason is null
                then 'Não houve'
                else scrapreason
            end as scraping
    from {{ ref('stg_workorder')}}
),

dates as (
    select 
        date_sk
        , cast(full_date as timestamp) as full_date
    from {{ ref('dim_dates')}}
),

final as (
    select 
        {{ dbt_utils.surrogate_key(['workorder.workorderid']) }} as workorder_sk
        , workrouting.workrouting_sk
        , locations.location_sk
        , products.product_sk
        , dates.date_sk
        , workorder.orderqty as qnt_produzida
        , workorder.scrappedqty as qtd_sucateada
        , workorder.scraping as razao_sucateamento
        , workorder.startdate as data_comeco
        , workorder.duedate as data_vencimento
        , workrouting.actualresourcehrs as horas_producao
        , workrouting.plannedcost as custo_plan_producao
        , workrouting.actualcost as custo_real_producao
    from workorder
    left join workrouting on workorder.workorderid = workrouting.workorderid
    left join locations on workrouting.locationid = locations.locationid
    left join products on workorder.productid = products.id_produto
    left join dates on workorder.startdate = dates.full_date
)

select *
from final