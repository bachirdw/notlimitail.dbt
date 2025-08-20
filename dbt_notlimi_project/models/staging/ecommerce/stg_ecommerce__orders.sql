with source as (

    select * from {{ source('ecommerce', 'orders') }}

),

renamed as (

    select
        -- Clés
        order_id,
        user_id,

        -- Informations sur la commande
        status,
        
        -- Horodatages
        created_at as created_at_utc,
        returned_at as returned_at_utc,
        shipped_at as shipped_at_utc,
        delivered_at as delivered_at_utc

        -- Nous excluons intentionnellement num_of_item.
        -- Nous le recalculerons dans la couche Silver pour garantir sa fiabilité.

    from source

)

select * from renamed