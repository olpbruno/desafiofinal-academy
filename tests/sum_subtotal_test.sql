with 
    sum_total as (
        select 
            sum(valor_total) as total
        from {{ref('fact_orders')}}
        where data_pedido between '2012-06-01' and '2012-06-30'
    )
    
select * from sum_total where total != 4610647.2153
