with campaigns_seed as (

    select * from {{ ref('stg_campaigns') }}
),

final as (
    select
        -- Clé primaire
        campaign_id,

        -- Attributs descriptifs de la campagne
        campaign_name,
        --campaign_type,

        -- Dates de la campagne, on s'assure qu'elles sont bien au format DATE
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date

    from campaigns_seed
)

select * from final