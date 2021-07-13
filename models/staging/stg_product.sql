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
        source_product.productid as id_produto
        , source_product.productnumber as numero_produto
        , source_product.name as produto
        , source_productcategory.category as categoria
        , source_productsubcategory.subcategory as subcategoria
        , source_productmodel.model as modelo
        , source_productdescription.description as descricao
        , case 
            when source_product.makeflag is true
                then 'Sim' 
                else 'Não'
            end as produto_comprado
        , case
            when source_product.finishedgoodsflag is true
                then 'Sim'
                else 'Não'
            end as venda_disponivel
        , source_product.color as cor
        , source_product.safetystocklevel as estoque_minimo
        , source_product.reorderpoint as estoque_pedido /* estoque minimo que aciona um pedido de compra ou produção */
        , source_product.standardcost as preco_padrao
        , source_product.listprice as valor_venda
        , source_product.size as tamanho_produto
        , source_product.sizeunitmeasurecode as medida_tamanho
        , source_product.weight as peso
        , source_product.weightunitmeasurecode as medida_peso
        , source_product.daystomanufacture as tempo_producao
        , source_product.productline as linha_produto
        , source_product.class as classe_produto
        , source_product.style as estilo
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