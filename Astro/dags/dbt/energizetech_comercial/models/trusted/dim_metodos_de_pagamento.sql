
{{config({
    "materialized": "incremental",
    "unique_key": "metodo_pagamento"
})}}

with dim_metodo_pagamento as (
    select distinct metodo_pagamento from {{ ref('staging_transacoes') }}
)

select 
    row_number() over (order by metodo_pagamento) as sk_metodo_pagamento,
    metodo_pagamento
from dim_metodo_pagamento