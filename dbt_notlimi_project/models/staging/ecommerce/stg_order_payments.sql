--

with source as (

    -- On référence la table brute des paiements
    select * from {{ source('olist_ecommerce', 'order_payments') }}

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