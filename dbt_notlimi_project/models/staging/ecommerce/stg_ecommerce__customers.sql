-- DANS models/staging/ecommerce/stg_ecommerce__customers.sql

with source as (

    select * from {{ source('olist_ecommerce', 'customers') }}

),

renamed as (

    select 
        -- Clés
        customer_id,
        customer_unique_id,

        -- Informations sur la région du client
        customer_zip_code_prefix,
        customer_city,
        customer_state

    from source

)

select * from renamed