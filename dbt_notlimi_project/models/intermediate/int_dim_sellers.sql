with sellers as (
    select * from {{ ref('stg_sellers') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

-- Brique 2 : Pré-calcul des métriques de vente par vendeur
seller_sales_facts as (
    select
        seller_id,
        count(order_item_id) as number_of_items_sold,
        sum(price) as total_revenue,
        min(shipping_limit_date) as first_sale_date,
        max(shipping_limit_date) as most_recent_sale_date
    from order_items
    -- On s'assure de ne pas prendre en compte les lignes où le seller_id serait manquant
    where seller_id is not null
    group by 1
),

-- Brique 3 : L'assemblage final
final as (
    select
        -- Informations de base du vendeur
        s.seller_id,
        s.seller_city,
        s.seller_state,
        
        -- Métriques de vente calculées
        coalesce(ssf.number_of_items_sold, 0) as number_of_items_sold,
        coalesce(ssf.total_revenue, 0) as total_revenue,
        ssf.first_sale_date,
        ssf.most_recent_sale_date

    from sellers as s
    
    -- On joint avec les faits de vente
    left join seller_sales_facts as ssf on s.seller_id = ssf.seller_id
)

select * from final