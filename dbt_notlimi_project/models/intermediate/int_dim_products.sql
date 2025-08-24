-- On commence par créer des CTEs pour nos deux sources de données
with products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

-- Brique 2 : Pré-calcul des métriques de vente par produit
product_sales_facts as (
    select
        product_id,
        count(order_item_id) as number_of_sales,
        sum(price) as total_revenue,
        min(shipping_limit_date) as first_sale_date, -- On utilise shipping_limit_date comme proxy
        max(shipping_limit_date) as most_recent_sale_date
    from order_items
    group by 1
),

-- Brique 3 : L'assemblage final
final as (
    select
        -- Informations de base du produit
        p.product_id,
        coalesce(p.product_category_name, 'unknown') as product_category,
        
        -- Métriques de vente calculées
        coalesce(psf.number_of_sales, 0) as number_of_sales,
        coalesce(psf.total_revenue, 0) as total_revenue,
        psf.first_sale_date,
        psf.most_recent_sale_date

    from products as p
    
    -- On joint avec les faits de vente
    left join product_sales_facts as psf on p.product_id = psf.product_id
)

select * from final