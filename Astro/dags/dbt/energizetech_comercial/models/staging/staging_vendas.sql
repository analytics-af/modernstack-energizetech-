with fonte_vendas as (
    select * from {{ source('public', 'sales')}}
)

select
    cast(sale_id as integer) AS id_venda,
    cast(customer_id as integer) AS id_cliente,
    cast(product_id as integer) AS id_produto,
    cast(sale_date as timestamp) AS data_venda,
    cast(quantity as integer) AS quantidade,
    cast(total_amount as decimal(10,2)) AS valor_total,
    cast(region as varchar) AS regiao,
    cast(salesperson_id as integer) AS id_vendedor,
    cast(payment_method as varchar) AS metodo_pagamento,
    cast(discount as decimal(5,2)) AS desconto
from fonte_vendas v
