-- 

with source as (

    select * from {{ source('olist_ecommerce', 'orders') }}

),

renamed as (

    select
        -- Clés
        order_id,
        customer_id, -- La colonne s'appelle customer_id,

        -- Informations sur la commande
        order_status,
        
        -- Lorsque l'on enregistre la date et l'heure d'un événement, on parle d'horodatage
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date

    from source

)

select * from renamed