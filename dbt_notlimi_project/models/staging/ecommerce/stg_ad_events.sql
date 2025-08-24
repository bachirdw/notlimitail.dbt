-- DANS models/staging/ads/stg_ad_events.sql

with source as (

    -- On utilise la fonction ref() car 'ad_events' est maintenant un "modèle" dbt (un seed)
    select * from {{ source('olist_ecommerce', 'ad_events') }}
),

renamed as (

    select
        -- Clés
        event_id,
        user_unique_id,
        campaign_id,
        product_id,

        -- Informations sur l'événement
        event_type,
        
        -- Horodatagex

        -- On s'assure que la colonne est bien de type TIMESTAMP
        cast(event_timestamp as timestamp) as event_timestamp_utc

    from source

)

select * from renamed