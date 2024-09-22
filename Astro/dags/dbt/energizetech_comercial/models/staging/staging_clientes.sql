with fonte_clientes as (
    select * from {{ source('public', 'customers') }}
)

select
    cast(customer_id as integer) AS id_cliente,
    cast(c.name as varchar) AS nome,
    cast(email as varchar) AS email,
    cast(phone as varchar) AS telefone,
    cast(c.address as varchar) AS endereco,
    cast(registration_date as timestamp) AS data_cadastro,
    cast(customer_type as varchar) AS tipo_cliente,
    cast(preferred_payment_method as varchar) AS metodo_pagamento_preferido
from fonte_clientes c
