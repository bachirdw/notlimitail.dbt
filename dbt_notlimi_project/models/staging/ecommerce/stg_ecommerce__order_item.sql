-- DANS models/staging/ecommerce/stg_ecommerce__order_item.sql

with source as (

    -- On référence la table brute des articles de commande
    select * from {{ source('olist_ecommerce', 'order_items') }}

),

renamed as (

    select
        -- Clés
        order_id,
        order_item_id,
        product_id,
        seller_id,

        -- Informations sur le prix et la livraison
        price,
        freight_value,
        
        -- Horodatage
        shipping_limit_date

    from source

)

select * from renamed