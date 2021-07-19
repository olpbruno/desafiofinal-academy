with product as (
    select * 
    from {{ ref('stg_product')}}
),

productmodel as (
    select *
    from {{ ref('stg_productmodel') }} 
),

productmodelproductdescriptionculture as (
    select *
    from {{ ref('stg_productmodelproductdescriptionculture') }} 
),

productdescription as (
    select *
    from {{ ref('stg_productdescription') }}
),

productsubcategory as (
    select *
    from {{ ref('stg_productsubcategory') }}
),

productcategory as (
    select *
    from {{ ref('stg_productcategory') }}
),

tabela_join as (
    select 
        product.sk_produto
        , product.id_produto
        , product.numero_produto
        , product.produto
        , product.produto_comprado
        , product.venda_disponivel
        , product.cor
        , product.estoque_minimo
        , product.estoque_pedido /* estoque minimo que aciona um pedido de compra ou produção */
        , product.preco_padrao
        , product.valor_venda
        , product.tamanho_produto
        , product.medida_tamanho
        , product.peso
        , product.medida_peso
        , product.tempo_producao
        , product.linha_produto
        , product.classe_produto
        , product.estilo
        , coalesce(productcategory.categoria,'Not Informed') as categoria
        , productsubcategory.subcategoria
        , productmodel.modelo
        , productdescription.descricao
    from product
    left join productsubcategory on product.sk_subcategoria = productsubcategory.sk_subcategoria
    left join productcategory on productsubcategory.sk_categoria = productcategory.sk_categoria
    left join productmodel on product.sk_modelo = productmodel.sk_modelo
    left join productmodelproductdescriptionculture on productmodel.sk_modelo = productmodelproductdescriptionculture.sk_modelo
    left join productdescription on productmodelproductdescriptionculture.sk_descricao = productdescription.sk_descricao
)

select * 
from tabela_join

