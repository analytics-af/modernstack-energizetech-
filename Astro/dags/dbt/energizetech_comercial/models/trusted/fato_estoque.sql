
{{config({
    "materialized": "table"
})}}

with fato_estoque as (
    select * from {{ ref('staging_estoque') }}
),

dim_produtos as (
    select * from {{ ref('dim_produtos') }}
)

select
    sk_produto,
    id_armazem,
    nivel_estoque,
    ponto_reabastecimento,
    valor_inventario,
    ultima_atualizacao
from fato_estoque d left join dim_produtos p on d.id_produto = p.id_produto
