-- ====================================================================
-- Configuration dbt pour le Partitionnement et le Clustering
-- ====================================================================


--materialized='incremental/ C'est une configuration avancée. 
--Elle dit à dbt de ne pas reconstruire toute la table à chaque fois, mais d'ajouter uniquement les nouvelles données
{{
    config(
        materialized='incremental',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["campaign_id"]
    )
}}

with ad_events as (
    select * from {{ ref('int_dim_ad_events') }}
),

orders as (
    select * from {{ ref('int_dim_orders') }}
),

-- Étape 1 : Agréger les événements publicitaires par jour et par campagne
daily_ad_events as (
    select
        cast(event_timestamp_utc as date) as event_date,
        campaign_id,
        count(case when event_type = 'impression' then event_id else null end) as impressions,
        count(case when event_type = 'click' then event_id else null end) as clicks
    from ad_events
    group by 1, 2
),

-- Étape 2 : Agréger les ventes par jour
daily_sales as (
    select
        cast(order_purchase_timestamp as date) as order_date,
        count(distinct order_id) as total_orders,
        sum(price) as total_revenue
    from orders
    group by 1
),

-- Étape 3 : Joindre les métriques publicitaires et les métriques de ventes par jour
final as (
    select
        -- Axe d'analyse principal
        dae.event_date,
        dae.campaign_id,

        -- Métriques publicitaires
        dae.impressions,
        dae.clicks,

        -- Métriques de vente (jointes par date)
        coalesce(ds.total_orders, 0) as total_orders,
        coalesce(ds.total_revenue, 0) as total_revenue

    from daily_ad_events as dae
    left join daily_sales as ds on dae.event_date = ds.order_date
)

select * from final