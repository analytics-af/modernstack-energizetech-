with fonte_produtos as (
    select * from {{ source('public', 'products') }}
)

select
    cast(product_id as integer) AS id_produto,
    cast(p.name as varchar) AS nome_produto,
    cast(category as varchar) AS categoria,
    cast(price as decimal(10,2)) AS preco,
    cast(stock_quantity as integer) AS quantidade_estoque,
    cast(supplier as varchar) AS fornecedor,
    cast(warranty_period as varchar) AS periodo_garantia,
    cast(battery_capacity as varchar) AS capacidade_bateria,
    cast(voltage as decimal(5,2)) AS voltagem
from fonte_produtos p
