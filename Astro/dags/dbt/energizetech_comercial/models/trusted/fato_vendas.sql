{{config({
    "materialized": "incremental"
})}}

with fato_vendas as (
    select * from {{ ref('staging_vendas') }}
),

staging_transacoes as (
    select * from {{ ref('staging_transacoes') }}
),

dim_produtos as (
    select * from {{ ref('dim_produtos') }}
),

dim_clientes as (
    select * from {{ ref('dim_clientes') }}
),

dim_vendedores as (
    select * from {{ ref('dim_vendedores') }}
)

select
    sk_produto,
    sk_cliente,
    sk_vendedor,
    v.data_venda,
    t.data_transacao,
    t.metodo_pagamento,
    t.status_transacao,
    v.quantidade,
    v.valor_total,
    v.valor_total / v.quantidade as valor_unitario,
    v.desconto
from fato_vendas v left join dim_produtos p on v.id_produto = p.id_produto
left join dim_clientes c on v.id_cliente = c.id_cliente and (v.data_venda >= c.data_inicio_validade and v.data_venda < c.data_fim_validade)
left join dim_vendedores s on v.id_vendedor = s.id_vendedor
left join staging_transacoes t on v.id_venda = t.id_venda