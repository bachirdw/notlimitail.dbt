-- On commence par créer des CTEs pour nos tables de staging (Bronze)
with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_payments as (
    select * from {{ ref('stg_order_payments') }}
),

-- On ne fait que des jointures, pas d'agrégation.
-- Le grain de cette table est l'article de commande (order_item).
final as (
    select
        -- Clés
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.seller_id,
        o.customer_id,

        -- Informations sur la commande
        o.order_status,
        o.order_purchase_timestamp,
        
        -- Informations sur l'article
        oi.price,
        oi.freight_value,

        -- Informations sur le paiement (au niveau de la transaction de paiement)
        op.payment_type,
        op.payment_installments,
        op.payment_value

    from order_items as oi
    
    left join orders as o on oi.order_id = o.order_id
    left join order_payments as op on oi.order_id = op.order_id
)

select * from final