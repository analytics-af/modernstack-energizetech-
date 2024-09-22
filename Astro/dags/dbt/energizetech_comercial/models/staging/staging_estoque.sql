with fonte_estoque as (
    select * from {{ source('public', 'inventory') }}
)

select
    cast(inventory_id as integer) AS id_inventario,
    cast(product_id as integer) AS id_produto,
    cast(warehouse_id as integer) AS id_armazem,
    cast(stock_level as integer) AS nivel_estoque,
    cast(reorder_point as integer) AS ponto_reabastecimento,
    cast(inventory_value as decimal(10,2)) AS valor_inventario,
    cast(last_update as timestamp) AS ultima_atualizacao
from fonte_estoque

