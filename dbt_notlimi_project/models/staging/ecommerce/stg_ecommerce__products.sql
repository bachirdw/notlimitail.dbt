-- DANS models/staging/ecommerce/stg_ecommerce__products.sql

with source as (

    -- On référence la table brute des produits
    select * from {{ source('olist_ecommerce', 'products') }}

),

renamed as (

    select
        -- Clé
        product_id,

        -- Informations sur le produit
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        
        -- Dimensions physiques du produit
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm

    from source

)

select * from renamed