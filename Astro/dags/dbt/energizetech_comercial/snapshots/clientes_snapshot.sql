{% snapshot clientes_snapshot %}


{{
    config(
      target_database='postgres',
      target_schema='dw_comercial',
      unique_key='id_cliente',
      strategy='check',
      check_cols=['nome'],
      invalidate_hard_deletes=True,
    )
}}

select
    id_cliente,
    nome,
    email,
    telefone,
    endereco,
    data_cadastro,
    tipo_cliente,
    metodo_pagamento_preferido
from {{ ref('staging_clientes') }}
{% endsnapshot %}
