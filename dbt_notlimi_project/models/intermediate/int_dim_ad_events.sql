with stg_ad_events as (
    select * from {{ ref('stg_ad_events') }}
),

final as (
    -- Pour l'instant, cette table intermédiaire est une copie propre de la table de staging.
    -- Plus tard, on pourrait y ajouter des jointures pour enrichir les événement
    select
        event_id,
        user_unique_id,
        campaign_id,
        product_id,
        event_type,
        event_timestamp_utc
    from stg_ad_events
)

select * from final