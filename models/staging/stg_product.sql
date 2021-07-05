with source_product as (
    select *	
    from {{ source('adventure_works','product') }}
),

source_billofmaterials as (
    select *
    from {{ source('adventure_works','billofmaterials') }}   
),

source_productmodel as (
    select 
        productmodelid,
        name as model
    from {{ source('adventure_works','productmodel') }} 
),

source_productmodelproductdescriptionculture as (
    select 
        productmodelid,
        productdescriptionid
    from {{ source('adventure_works','productmodelproductdescriptionculture') }} 
),

source_productdescription as (
    select
        pd.productdescriptionid,
        pd.description as description
    from {{ source('adventure_works','productdescription') }} as pd
),

source_productsubcategory as (
    select
        psc.productsubcategoryid,
        psc.productcategoryid,
        psc.name as subcategory
    from {{ source('adventure_works','productsubcategory') }} as psc
),

source_productcategory as (
    select
        pc.productcategoryid,
        pc.name as category
    from {{ source('adventure_works','productcategory') }} as pc
),

final as (
    select
        productid,
        productnumber,
        name,
        category,
        subcategory,
        model,
        description,
        makeflag,
        finishedgoodsflag,
        color,
        safetystocklevel,
        reorderpoint,
        standardcost,
        listprice,
        size,
        sizeunitmeasurecode,
        weightunitmeasurecode,
        weight,
        daystomanufacture,
        productline,
        class,
        style,
        sellstartdate,
        sellenddate,
        from source_product
        left join source_billofmaterials on source_product.productid = source_billofmaterials.productassemblyid 
        left join source_productsubcategory on source_product.productsubcategoryid = source_productsubcategory.productsubcategoryid
        left join source_productcategory on source_productsubcategory.productcategoryid = source_productcategory.productcategoryid
        left join source_productmodel on source_product.productmodelid = source_productmodel.productmodelid
        left join source_productmodelproductdescriptionculture on source_productmodel.productmodelid = source_productmodelproductdescriptionculture.productdescriptionid
        left join source_productdescription on source_productmodelproductdescriptionculture.productdescriptionid = source_productdescription.productdescriptionid
)

select * 
from final