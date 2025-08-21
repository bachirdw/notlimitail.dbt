-- DANS models/staging/ecommerce/stg_ecommerce__sellers.sql

with source as (

    -- On référence la table brute des vendeurs
    select * from {{ source('olist_ecommerce', 'sellers') }}

),

renamed as (

    select
        -- Clé
        seller_id,

        -- Informations de localisation du vendeur
        seller_zip_code_prefix,
        seller_city,
        seller_state

    from source

)

select * from renamed