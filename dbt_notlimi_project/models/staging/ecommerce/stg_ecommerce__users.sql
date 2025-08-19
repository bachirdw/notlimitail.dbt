with source as (

    -- On utilise la fonction source() pour référencer la table brute des utilisateurs
    select * from {{ source('ecommerce', 'users') }}

),

renamed_and_cleaned as (

    select
        -- Clés
        id as user_id,

        -- Informations sur l'utilisateur
        first_name,
        last_name,
        -- On nettoie l'email en le mettant en minuscule et en enlevant les espaces
        trim(lower(email)) as email,
        age,
        gender,

        -- Informations de localisation
        state,
        city,
        country,

        -- Informations d'acquisition
        traffic_source,

        -- Horodatages
        created_at as created_at_utc

    from source

)

select * from renamed_and_cleaned