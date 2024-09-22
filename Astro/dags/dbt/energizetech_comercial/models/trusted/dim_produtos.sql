--Dimensão de Produtos - SCD type 1 - Only Update

{{ config({
    "materialized": "incremental",
    "unique_key": "id_produto"
}) }}

with dim_produtos as (
    select * from {{ ref('staging_produtos') }}
)

select 
    id_produto as sk_produto,
    id_produto, 
    nome_produto, 
    categoria, 
    preco, 
    quantidade_estoque, 
    fornecedor, 
    periodo_garantia, 
    capacidade_bateria, 
    voltagem 
from dim_produtos
