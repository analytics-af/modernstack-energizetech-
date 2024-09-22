
{{config({
    "materialized": "incremental",
    "unique_key": "id_vendedor"
})}}

with dim_vendedores as (
    select * from {{ ref('staging_vendedores') }}
)

select 
    id_vendedor as sk_vendedor,
    id_vendedor,
    nome_vendedor,
    email,
    telefone,
    regiao,
    data_contratacao
from dim_vendedores