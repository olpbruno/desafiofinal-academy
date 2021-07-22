with source_data as (
    select
        productid as id_produto
        , productnumber as numero_produto
        , name as produto
        , case 
            when makeflag is true
                then 'Sim' 
                else 'Não'
            end as produto_comprado
        , case
            when finishedgoodsflag is true
                then 'Sim'
                else 'Não'
            end as venda_disponivel
        , coalesce(color, 'Not Informed') as cor
        , safetystocklevel as estoque_minimo
        , reorderpoint as estoque_pedido /* estoque minimo que aciona um pedido de compra ou produção */
        , standardcost as preco_padrao
        , listprice as valor_venda
        , size as tamanho_produto
        , sizeunitmeasurecode as medida_tamanho
        , weight as peso
        , weightunitmeasurecode as medida_peso
        , daystomanufacture as tempo_producao
        , productline as linha_produto
        , class as classe_produto
        , style as estilo
        , productmodelid as id_modelo
        , productsubcategoryid as id_subcategoria
    from {{ source('adventure_works','product') }} 
),

source_with_sk as(
    select *
        , {{ dbt_utils.surrogate_key(['id_produto']) }} as sk_produto
        , {{ dbt_utils.surrogate_key(['id_modelo']) }} as sk_modelo
        , {{ dbt_utils.surrogate_key(['id_subcategoria']) }} as sk_subcategoria
    from source_data
)

select * 
from source_with_sk