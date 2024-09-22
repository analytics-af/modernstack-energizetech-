with fonte_devolucoes as (
    select * from {{ source('public', 'returns') }}
)

select
    cast(return_id as integer) AS id_devolucao,
    cast(sale_id as integer) AS id_venda,
    cast(customer_id as integer) AS id_cliente,
    cast(product_id as integer) AS id_produto,
    cast(return_date as timestamp) AS data_devolucao,
    cast(quantity_returned as integer) AS quantidade_devolvida,
    cast(return_amount as decimal(10,2)) AS valor_devolucao,
    cast(return_reason as varchar) AS motivo_devolucao
from fonte_devolucoes
