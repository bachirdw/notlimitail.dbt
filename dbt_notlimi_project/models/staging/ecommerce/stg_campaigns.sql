with source as (
    -- On lit depuis le SEED qui DÉCRIT les campagnes
 select * from {{ source('olist_ecommerce', 'campaigns') }}

),

renamed_and_typed as (
    select
        campaign_id,
        campaign_name,
        product_category,
        -- On fait le typage ici, dans la couche staging
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date
    from source
)

select * from renamed_and_typed