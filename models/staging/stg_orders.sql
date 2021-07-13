with source_orderheader as (
    select 
        salesorderid
        , revisionnumber
        , orderdate
        , duedate
        , shipdate
        , status
        , onlineorderflag
        , purchaseordernumber
        , accountnumber
        , customerid
        , salespersonid
        , territoryid
        , billtoaddressid
        , shiptoaddressid
        , shipmethodid
        , creditcardid
        , creditcardapprovalcode
        , currencyrateid
        , taxamt
        , freight
        , totaldue
        , comment
        , rowguid
        , modifieddate
        , {{dbt_utils.surrogate_key(['salesorderid'])}} as order_sk
    from {{ source('adventure_works','salesorderheader') }}
),

source_orderdetail as (
    select 
        salesorderid
        , salesorderdetailid
        , carriertrackingnumber
        , orderqty
        , productid
        , specialofferid
        , unitprice
        , unitpricediscount
        , sum(sc_od.unitprice*(1 - sc_od.unitpricediscount)*sc_od.orderqty) as subtotal
        , {{dbt_utils.surrogate_key(['salesorderdetailid'])}} as orderdetail_sk
    from {{ source('adventure_works','salesorderdetails') }}
    group by 
        salesorderid
        , salesorderdetailid
        , carriertrackingnumber
        , orderqty
        , productid
        , specialofferid
        , unitprice
        , unitpricediscount
),

source_salesorderheadersalesreason as (
    select *
    from {{ source('adventure_works','salesorderheadersalesreason') }}
),

source_salesreason as (
    select *
    from {{ source('adventure_works','salesreason') }}
),

orders as (
    select 
        source_orderheader.*
        , sc_od.orderdetail_sk
        , sc_od.orderqty
        , sc_od.productid
        , sc_od.unitprice
        , sc_od.unitpricediscount
        , sc_od.subtotal
        , source_salesreason.salesreasonid
    from source_orderheader
    left join source_orderdetail as sc_od on source_orderheader.salesorderid = sc_od.salesorderid
    left join source_salesorderheadersalesreason on source_orderheader.salesorderid = source_salesorderheadersalesreason.salesorderid
    left join source_salesreason on source_salesorderheadersalesreason.salesreasonid = source_salesreason.salesreasonid
)

select * from orders