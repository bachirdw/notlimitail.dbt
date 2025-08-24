-- On commence par créer des CTEs pour nos tables de staging (Bronze)
with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

-- On ne fait que des jointures. Pas de GROUP BY, MIN, MAX, ou COUNT.

final as (
    select
        -- Clés
        o.order_id,
        o.customer_id,
        c.customer_unique_id,

        -- Informations sur le client (qui viennent de la jointure)
        c.customer_city,
        c.customer_state,

        -- Informations sur la commande
        o.order_status,
        o.order_purchase_timestamp

    from orders as o
    
    -- On attache les informations du client à chaque commande
    left join customers as c on o.customer_id = c.customer_id
)

select * from final