/*
 Références aux Staging : Il commence par lire les tables Bronze (stg_...) que nous avons préparées.
Pré-Agrégations : Avant de joindre, il fait des agrégations logiques :
Il calcule le montant total payé par type de paiement pour chaque commande (order_payments_aggregated). C'est une version améliorée du modèle "pivot" que nous avions vu.
Il calcule le nombre d'articles et la valeur totale des produits pour chaque commande (order_items_aggregated).
Jointures : Il assemble ensuite la table principale orders avec ces deux nouvelles tables agrégées. On utilise des LEFT JOIN pour être sûr de ne perdre aucune commande, même si (par miracle) elle n'avait pas d'articles ou de paiement.
Nettoyage final : La fonction coalesce(colonne, 0) est utilisée pour remplacer les NULL par des 0. C'est une bonne pratique pour s'assurer que les colonnes numériques sont toujours des nombres, ce qui facilite les calculs dans la couche Gold.
Résultat : La table finale, int_orders_with_details, contiendra une seule ligne par commande, avec toutes les métriques importantes déjà calculées (nombre d'articles, montant total, etc.). C'est une table "Silver" parfaite, propre et fiable
 */

with
orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_payments as (
    select * from {{ ref('stg_order_payments') }}
),

-- On agrège d'abord les paiements pour avoir une seule ligne par commande
order_payments_aggregated as (
    select
        order_id,
        sum(case when payment_type = 'credit_card' then payment_value else 0 end) as credit_card_amount,
        sum(case when payment_type = 'boleto' then payment_value else 0 end) as boleto_amount,
        sum(case when payment_type = 'voucher' then payment_value else 0 end) as voucher_amount,
        sum(case when payment_type = 'debit_card' then payment_value else 0 end) as debit_card_amount,
        sum(payment_value) as total_amount
    from order_payments
    group by 1
),

-- On agrège les articles pour avoir des métriques par commande
order_items_aggregated as (
    select
        order_id,
        count(order_item_id) as number_of_items,
        sum(price) as total_price,
        sum(freight_value) as total_freight_value
    from order_items
    group by 1
),

final as (
    select
        -- Clés
        o.order_id,
        o.customer_id,

        -- Informations sur la commande
        o.order_status,
        
        -- Horodatages
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        
        -- Métriques des articles
        coalesce(oi.number_of_items, 0) as number_of_items,
        coalesce(oi.total_price, 0) as total_price,
        coalesce(oi.total_freight_value, 0) as total_freight_value,

        -- Métriques des paiements
        coalesce(op.total_amount, 0) as total_amount_paid,
        coalesce(op.credit_card_amount, 0) as credit_card_amount,
        coalesce(op.boleto_amount, 0) as boleto_amount,
        coalesce(op.voucher_amount, 0) as voucher_amount,
        coalesce(op.debit_card_amount, 0) as debit_card_amount

    from orders o
    left join order_items_aggregated oi on o.order_id = oi.order_id
    left join order_payments_aggregated op on o.order_id = op.order_id
)

select * from final