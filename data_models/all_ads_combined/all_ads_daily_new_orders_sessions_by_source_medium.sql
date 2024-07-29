-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +all_ads_daily_new_orders_sessions_by_source_medium -t dbt_transform

{{ config(materialized='table') }}

with 
-- input
    ga4_daily_country_channel_revenue as (select * from {{ ref('ga4_daily_country_channel_revenue') }}),

    daily_new_return_sales_orders as (select * from {{ ref('daily_new_return_sales_orders') }}),

    ga_4 as (
    select
        date,
        region,
        ga4_source_medium                                   as source_medium,
        round(sum(sessions))                                as sessions
    from 
        ga4_daily_country_channel_revenue
    group by 
        date,
        region,
        ga4_source_medium
    ),

    shopify as (
    select
        day                                                 as date,
        region,
        'not_specified'                                     as source_medium,
        round(sum(new_orders))                              as new_orders
    from daily_new_return_sales_orders
    group by 
        day,
        region,
        source_medium
    ),

    join_shopify as (
    select
        coalesce(ga_4.date, shopify.date)                   as date,
        coalesce(ga_4.region, shopify.region)               as region,
        coalesce(ga_4.source_medium, shopify.source_medium) as source_medium,
        ga_4.sessions,
        shopify.new_orders
    from 
        shopify full outer join ga_4
        using (
            date,
            region,
            source_medium
        )
    )

select
  *
from join_shopify

