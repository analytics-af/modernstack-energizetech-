with fonte_transacoes as (
    select * from {{ source('public', 'transactions') }}
)

select
    cast(transaction_id as integer) AS id_transacao,
    cast(sale_id as integer) AS id_venda,
    cast(transaction_date as timestamp) AS data_transacao,
    cast(payment_method as varchar) AS metodo_pagamento,
    cast(amount as decimal(10,2)) AS valor,
    cast(f.status as varchar) AS status_transacao
from fonte_transacoes f
