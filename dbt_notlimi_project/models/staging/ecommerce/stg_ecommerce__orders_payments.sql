-- DANS models/staging/ecommerce/stg_ecommerce__order_payments.sql

with source as (

    -- On référence la table brute des paiements
    select * from {{ source('olist_ecommerce', 'orders_payments') }}

),

renamed as (

    select
        -- Clés
        order_id,

        -- Informations sur le paiement
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value

    from source

)

select * from renamed