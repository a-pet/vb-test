-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +all_ads_daily_performance -t dbt_transform

{{ config(materialized='table')}}

with 
-- input
    daily_meta_ads as ( select * from {{ ref('daily_meta_ads') }}),

    daily_google_ads as ( select * from {{ ref('daily_google_ads') }}),

    ga4_daily_country_channel_revenue as ( select * from {{ ref('ga4_daily_country_channel_revenue') }}),

    daily_new_return_sales_orders as ( select * from {{ ref('daily_new_return_sales_orders') }}),

    _ga4_metrics as (
        select
            date,

            sum(revenue)                                                                as ga4_sales,
            sum(purchasers)                                                             as ga4_orders,
            sum(sessions)                                                               as ga4_sessions,
            region,
            
            sum(
                if(
                    session_default_channel_group = 'Direct',
                    revenue,
                    0
                )
            )                                                                           as ga4_direct_revenue,
            
            sum(
                if(
                    session_default_channel_group = 'Direct',
                    purchasers,
                    0
                )
            )                                                                           as ga4_direct_orders,

            sum(
                if(
                    session_default_channel_group = 'Direct',
                    sessions,
                    0
                )
            )                                                                           as ga4_direct_sessions,

            sum(
                if(
                    session_default_channel_group = 'Referral',
                    revenue,
                    0
                )
            )                                                                           as ga4_referral_revenue,
            
            sum(
                if(
                    session_default_channel_group = 'Referral',
                    purchasers,
                    0
                )
            )                                                                           as ga4_referral_orders,

            sum(
                if(
                    session_default_channel_group = 'Referral',
                    sessions,
                    0
                )
            )                                                                           as ga4_referral_sessions,


            sum(
                if(
                    lower(session_default_channel_group) like '%organic%',
                    revenue,
                    0
                )
            )                                                                           as ga4_organic_revenue,
            
            sum(
                if(
                    lower(session_default_channel_group) like '%organic%',
                    purchasers,
                    0
                )
            )                                                                           as ga4_organic_orders,

            sum(
                if(
                    lower(session_default_channel_group) like '%organic%',
                    sessions,
                    0
                )
            )                                                                           as ga4_organic_sessions,

            sum(
                if(
                    ads_platform = 'Google Ads',
                    revenue,
                    0
                )
            )                                                                           as ga4_gads_revenue,

            sum(
                if(
                    ads_platform = 'Meta Ads',
                    revenue,
                    0
                )
            )  
            
            * 1.75                                                                      as ga4_meta_revenue,

            sum(
                if(
                    ads_platform = 'Adroll',
                    revenue,
                    0
                )
            )                                                                           as ga4_adroll_revenue,

            sum(
                if(
                    ads_platform = 'Pinterest Ads',
                    revenue,
                    0
                )
            )                                                                           as ga4_pinterest_revenue,

            sum(
                if(
                    ads_platform = 'Attentive',
                    revenue,
                    0
                )
            )                                                                           as ga4_attentive_revenue,

            sum(
                if(
                    ads_platform = 'Bing Ads',
                    revenue,
                    0
                )
            )                                                                           as ga4_bing_revenue

        from ga4_daily_country_channel_revenue

        group by 
            date,
            region
        having
            sum(revenue) != 0 and sum(purchasers) != 0
    ),

    _shopify_sales_orders as (
        select 
            day                                                                         as date,
            region,

            sum(new_sales + return_sales)                                               as shopify_sales,
            sum(new_sales)                                                              as shopify_new_sales,
            sum(return_sales)                                                           as shopify_return_sales,

            sum(new_orders + return_orders)                                             as shopify_orders,
            sum(new_orders)                                                             as shopify_new_orders,
            sum(return_orders)                                                          as shopify_return_orders

        from daily_new_return_sales_orders
        
        group by 
            date,
            region
    ),

    _meta_daily as (
        select 
            ad_run_date                                                                 as date,
            region,

            sum(impressions)                                                            as meta_impressions,
            sum(landing_page_views)                                                     as landing_page_views,
            sum(unique_inline_link_clicks)                                              as meta_clicks,
            sum(spend)                                                                  as meta_spend,
            sum(onsite_web_purchases)                                                   as meta_conversions,
            sum(onsite_web_app_purchase_sum_usd)                                        as meta_sales,

            
            sum(
                if(
                    is_retargeting,
                    landing_page_views,
                    0
                )
            )                                                                           as meta_landing_page_views_retargeting,

            sum(
                if(
                    is_prospecting,
                    landing_page_views,
                    0
                )
            )                                                                           as meta_landing_page_views_prospecting,

            sum(
                if(
                    is_experiment,
                    landing_page_views,
                    0
                )
            )                                                                           as meta_landing_page_views_experiment,

            sum(
                if(
                    is_reach_and_frequency,
                    landing_page_views,
                    0
                )
            )                                                                           as meta_landing_page_views_rf,
            
            ------------------------
            sum(
                if(
                    is_retargeting,
                    onsite_web_purchases,
                    0
                )
            )                                                                           as meta_conversions_retargeting,

            sum(
                if(
                    is_prospecting,
                    onsite_web_purchases,
                    0
                )
            )                                                                           as meta_conversions_prospecting,

            sum(
                if(
                    is_experiment,
                    onsite_web_purchases,
                    0
                )
            )                                                                           as meta_conversions_experiment,

            sum(
                if(
                    is_reach_and_frequency,
                    onsite_web_purchases,
                    0
                )
            )                                                                           as meta_conversions_rf,
            
            ----------------------
            sum(
                if(
                    is_retargeting,
                    impressions,
                    0
                )
            )                                                                           as meta_impressions_retargeting,

            sum(
                if(
                    is_prospecting,
                    impressions,
                    0
                )
            )                                                                           as meta_impressions_prospecting,

            sum(
                if(
                    is_experiment,
                    impressions,
                    0
                )
            )                                                                           as meta_impressions_experiment,

            sum(
                if(
                    is_reach_and_frequency,
                    impressions,
                    0
                )
            )                                                                           as meta_impressions_rf,
            ---------------------------
            sum(
                if(
                    is_retargeting,
                    unique_inline_link_clicks,
                    0
                )
            )                                                                           as meta_clicks_retargeting,

            sum(
                if(
                    is_prospecting,
                    unique_inline_link_clicks,
                    0
                )
            )                                                                           as meta_clicks_prospecting,

            sum(
                if(
                    is_experiment,
                    unique_inline_link_clicks,
                    0
                )
            )                                                                           as meta_clicks_experiment,

            sum(
                if(
                    is_reach_and_frequency,
                    unique_inline_link_clicks,
                    0
                )
            )                                                                           as meta_clicks_rf,
            -----------------
            sum(
                if(
                    is_retargeting,
                    spend,
                    0
                )
            )                                                                           as meta_spend_retargeting,

            sum(
                if(
                    is_prospecting,
                    spend,
                    0
                )
            )                                                                           as meta_spend_prospecting,

            sum(
                if(
                    is_experiment,
                    spend,
                    0
                )
            )                                                                           as meta_spend_experiment,

            sum(
                if(
                    is_reach_and_frequency,
                    spend,
                    0
                )
            )                                                                           as meta_spend_rf,

            sum(
                if(
                    is_retargeting,
                    onsite_web_app_purchase_sum_usd,
                    0
                )
            )                                                                           as meta_sales_retargeting,

            sum(
                if(
                    is_prospecting,
                    onsite_web_app_purchase_sum_usd,
                    0
                )
            )                                                                           as meta_sales_prospecting,

            sum(
                if(
                    is_experiment,
                    onsite_web_app_purchase_sum_usd,
                    0
                )
            )                                                                           as meta_sales_experiment,

            sum(
                if(
                    is_reach_and_frequency,
                    onsite_web_app_purchase_sum_usd,
                    0
                )
            )                                                                           as meta_sales_rf,

        from daily_meta_ads

        group by
            date,
            region
    ),
    
    
    _gads_daily as (
        select 
            ad_run_date                                                                 as date,
            region,
            
            sum(impressions)                                                            as gads_impressions,
            sum(clicks)                                                                 as gads_clicks,
            sum(spend)                                                                  as gads_spend,
            sum(conversions)                                                            as gads_conversions,
            sum(metrics_conversions_value)                                              as gads_sales,

            sum(
                if(
                    campaign_type = 'brand',
                    conversions,
                    0
                )
            )                                                                           as gads_conversions_brand,

            sum(
                if(
                    campaign_type = 'generic',
                    conversions,
                    0
                )
            )                                                                           as gads_conversions_generic,

            sum(
                if(
                    campaign_type = 'pmax',
                    conversions,
                    0
                )
            )                                                                           as gads_conversions_pmax,
            
            -----------------------------------
            sum(
                if(
                    campaign_type = 'brand',
                    clicks,
                    0
                )
            )                                                                           as gads_clicks_brand,

            sum(
                if(
                    campaign_type = 'generic',
                    clicks,
                    0
                )
            )                                                                           as gads_clicks_generic,

            sum(
                if(
                    campaign_type = 'pmax',
                    clicks,
                    0
                )
            )                                                                           as gads_clicks_pmax,

            -------------------------
            sum(
                if(
                    campaign_type = 'brand',
                    impressions,
                    0
                )
            )                                                                           as gads_impressions_brand,

            sum(
                if(
                    campaign_type = 'generic',
                    impressions,
                    0
                )
            )                                                                           as gads_impressions_generic,

            sum(
                if(
                    campaign_type = 'pmax',
                    impressions,
                    0
                )
            )                                                                           as gads_impressions_pmax,
            
            -----------------------------
            sum(
                if(
                    campaign_type = 'brand',
                    spend,
                    0
                )
            )                                                                           as gads_spend_brand,

            sum(
                if(
                    campaign_type = 'generic',
                    spend,
                    0
                )
            )                                                                           as gads_spend_generic,

            sum(
                if(
                    campaign_type = 'pmax',
                    spend,
                    0
                )
            )                                                                           as gads_spend_pmax,

            ---------------------------------------
            
            sum(
                if(
                    campaign_type = 'brand',
                    metrics_conversions_value,
                    0
                )
            )                                                                           as gads_sales_brand,

            sum(
                if(
                    campaign_type = 'generic',
                    metrics_conversions_value,
                    0
                )
            )                                                                           as gads_sales_generic,

            sum(
                if(
                    campaign_type = 'pmax',
                    metrics_conversions_value,
                    0
                )
            )                                                                           as gads_sales_pmax

        from daily_google_ads

        group by 
            date,
            region
    ),

    _join_vbb_report as (
        select
            *
        from _ga4_metrics full outer join _shopify_sales_orders
        using (
            date,
            region
        )
    ),

    _join_meta as (
        select
            *
        from _join_vbb_report full outer join _meta_daily
        using (
            date,
            region
        )
    ),

    _join_gads as (
        select
            *
        from _join_meta full outer join _gads_daily
        using (
            date,
            region
        )
    ),

    group_by_day as (
        select 
            date,
            date_trunc(date, month)                                                 as month,
            region,

            coalesce(sum(ga4_sessions), 0)                                          as ga4_sessions,
            coalesce(sum(ga4_orders), 0)                                            as ga4_orders,
            
            coalesce(sum(meta_impressions), 0)                                      as meta_impressions,
            coalesce(sum(meta_impressions_retargeting), 0)                          as meta_impressions_retargeting,
            coalesce(sum(meta_impressions_prospecting), 0)                          as meta_impressions_prospecting,
            coalesce(sum(meta_impressions_experiment), 0)                           as meta_impressions_experiment,
            coalesce(sum(meta_impressions_rf), 0)                                   as meta_impressions_rf,

            coalesce(sum(landing_page_views), 0)                                    as meta_landing_page_views,
            coalesce(sum(meta_landing_page_views_retargeting), 0)                   as meta_landing_page_views_retargeting,
            coalesce(sum(meta_landing_page_views_prospecting), 0)                   as meta_landing_page_views_prospecting,
            coalesce(sum(meta_landing_page_views_experiment), 0)                    as meta_landing_page_views_experiment,
            coalesce(sum(meta_landing_page_views_rf), 0)                            as meta_landing_page_views_rf,

            coalesce(sum(gads_impressions), 0)                                      as gads_impressions,
            coalesce(sum(gads_impressions_brand), 0)                                as gads_impressions_brand,
            coalesce(sum(gads_impressions_generic), 0)                              as gads_impressions_generic,
            coalesce(sum(gads_impressions_pmax), 0)                                 as gads_impressions_pmax,

            coalesce(sum(meta_clicks), 0)                                           as meta_clicks,
            coalesce(sum(meta_clicks_retargeting), 0)                               as meta_clicks_retargeting,
            coalesce(sum(meta_clicks_prospecting), 0)                               as meta_clicks_prospecting,
            coalesce(sum(meta_clicks_experiment), 0)                                as meta_clicks_experiment,
            coalesce(sum(meta_clicks_rf), 0)                                        as meta_clicks_rf,

            coalesce(sum(gads_clicks), 0)                                           as gads_clicks,
            coalesce(sum(gads_clicks_brand), 0)                                     as gads_clicks_brand,
            coalesce(sum(gads_clicks_generic), 0)                                   as gads_clicks_generic,
            coalesce(sum(gads_clicks_pmax), 0)                                      as gads_clicks_pmax,

            coalesce(sum(meta_conversions), 0)                                      as meta_conversions,
            coalesce(sum(meta_conversions_retargeting), 0)                          as meta_conversions_retargeting,
            coalesce(sum(meta_conversions_prospecting), 0)                          as meta_conversions_prospecting,
            coalesce(sum(meta_conversions_experiment), 0)                           as meta_conversions_experiment,
            coalesce(sum(meta_conversions_rf), 0)                                   as meta_conversions_rf,

            coalesce(sum(gads_conversions), 0)                                      as gads_conversions,
            coalesce(sum(gads_conversions_brand), 0)                                as gads_conversions_brand,
            coalesce(sum(gads_conversions_generic), 0)                              as gads_conversions_generic,
            coalesce(sum(gads_conversions_pmax), 0)                                 as gads_conversions_pmax,

            coalesce(sum(ga4_sales), 0)                                             as ga4_sales,
            coalesce(sum(ga4_gads_revenue), 0)                                      as ga4_gads_revenue,
            coalesce(sum(ga4_meta_revenue), 0)                                      as ga4_meta_revenue,

            coalesce(sum(ga4_direct_revenue), 0)                                    as ga4_direct_revenue,
            coalesce(sum(ga4_direct_orders), 0)                                     as ga4_direct_orders,
            coalesce(sum(ga4_direct_sessions), 0)                                   as ga4_direct_sessions,
            coalesce(sum(ga4_referral_revenue), 0)                                  as ga4_referral_revenue,
            coalesce(sum(ga4_referral_orders), 0)                                   as ga4_referral_orders,
            coalesce(sum(ga4_referral_sessions), 0)                                 as ga4_referral_sessions,
            coalesce(sum(ga4_organic_revenue), 0)                                   as ga4_organic_revenue,
            coalesce(sum(ga4_organic_orders), 0)                                    as ga4_organic_orders,
            coalesce(sum(ga4_organic_sessions), 0)                                  as ga4_organic_sessions,
            
            coalesce(sum(shopify_sales), 0)                                         as vbb_sales,
            coalesce(sum(shopify_new_sales), 0)                                     as vbb_new_sales,
            coalesce(sum(shopify_return_sales), 0)                                  as vbb_return_sales,
            coalesce(sum(shopify_orders), 0)                                        as vbb_orders,
            coalesce(sum(shopify_new_orders), 0)                                    as vbb_new_orders,
            coalesce(sum(shopify_return_orders), 0)                                 as vbb_return_orders,
            
            coalesce(sum(meta_sales), 0)                                            as meta_sales,
            coalesce(sum(meta_sales_retargeting), 0)                                as meta_sales_retargeting,
            coalesce(sum(meta_sales_prospecting), 0)                                as meta_sales_prospecting,
            coalesce(sum(meta_sales_experiment), 0)                                 as meta_sales_experiment,
            coalesce(sum(meta_sales_rf), 0)                                         as meta_sales_rf,
        
            coalesce(sum(gads_sales), 0)                                            as gads_sales,
            coalesce(sum(gads_sales_brand), 0)                                      as gads_sales_brand,
            coalesce(sum(gads_sales_generic), 0)                                    as gads_sales_generic,
            coalesce(sum(gads_sales_pmax), 0)                                       as gads_sales_pmax,

            coalesce(sum(meta_spend), 0)                                            as meta_spend,
            coalesce(sum(meta_spend_retargeting), 0)                                as meta_spend_retargeting,
            coalesce(sum(meta_spend_prospecting), 0)                                as meta_spend_prospecting,
            coalesce(sum(meta_spend_experiment), 0)                                 as meta_spend_experiment,
            coalesce(sum(meta_spend_rf), 0)                                         as meta_spend_rf,

            coalesce(sum(gads_spend), 0)                                            as gads_spend,
            coalesce(sum(gads_spend_brand), 0)                                      as gads_spend_brand,
            coalesce(sum(gads_spend_generic), 0)                                    as gads_spend_generic,
            coalesce(sum(gads_spend_pmax), 0)                                       as gads_spend_pmax

        from _join_gads
        
        group by
            date,
            region
    )

-- final
select * from group_by_day
