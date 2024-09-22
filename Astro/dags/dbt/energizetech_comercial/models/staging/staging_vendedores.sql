with fonte_vendedores as (
    select * from {{ source('public', 'salespersons')}}
)

select
    cast(salesperson_id as integer) AS id_vendedor,
    cast(v.name as varchar) AS nome_vendedor,
    cast(email as varchar) AS email,
    cast(phone as varchar) AS telefone,
    cast(region as varchar) AS regiao,
    cast(hire_date as date) AS data_contratacao,
    cast(total_sales as decimal(10,2)) AS total_vendas
from fonte_vendedores v
