--Dimensão de Cleintes - SCD type 2 com atributos de SCD 1 - União de Snapshot SC2 com Staging

with dim_clientes as (
    select * from {{ ref('staging_clientes') }}
),

snapshot_clientes as (
    select * from {{ ref('clientes_snapshot') }}
)

select 
    s.dbt_scd_id as sk_cliente,
    s.id_cliente, 
    s.nome,
    c.email,
    c.telefone,
    c.endereco,
    c.data_cadastro,
    c.tipo_cliente,
    c.metodo_pagamento_preferido,
    s.dbt_updated_at as data_atualizacao, 
    case
        when s.dbt_valid_from = (select min(dbt_valid_from) from snapshot_clientes)
        then '1900-01-01 00:00:00.0000'::timestamp
        else s.dbt_valid_from
    end as data_inicio_validade,
    case
        when s.dbt_valid_to is null
        then '2099-12-31 23:59:59.9999'::timestamp
        else s.dbt_valid_to
    end as data_fim_validade
from snapshot_clientes s
left join dim_clientes c on c.id_cliente = s.id_cliente
