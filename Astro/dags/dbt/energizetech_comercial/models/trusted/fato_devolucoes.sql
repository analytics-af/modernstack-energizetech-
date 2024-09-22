{{config({
    "materialized": "incremental"
})}}

with fato_devolucoes as (
    select * from {{ ref('staging_devolucoes') }}
),

dim_produto as (
    select * from {{ ref('dim_produtos') }}
),

dim_cliente as (
    select * from {{ ref('dim_clientes') }}
)

select
    sk_produto,
    sk_cliente,
    data_devolucao,
    quantidade_devolvida,
    valor_devolucao,
    motivo_devolucao
from fato_devolucoes d left join dim_produto p on d.id_produto = p.id_produto
left join dim_cliente c on d.id_cliente = c.id_cliente and (d.data_devolucao >= c.data_inicio_validade and d.data_devolucao < c.data_fim_validade)


