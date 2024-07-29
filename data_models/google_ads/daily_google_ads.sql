-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +daily_google_ads+ -t dbt_transform

{{ config(materialized='view')}}

with
-- input
    source_daily_google_ads_keywords as (
        select * from {{ source('sources_combined', 'source_daily_google_ads_keywords') }}
    ),

    source_hourly_google_ads_campaigns as (
        select * from {{ source('sources_combined', 'source_hourly_google_ads_campaigns') }}
    ),

    source_daily_google_ads_ad_group_with_more_stats as (
        select * from {{ source('sources_combined', 'source_daily_google_ads_ad_group_with_more_stats') }}
    ),

    google_ads_campaigns as (
        select * from {{ ref('google_ads_campaigns') }}
    ),

    intermediate_google_ads_group_with_product_categories as (
        select * from {{ ref('intermediate_google_ads_group_with_product_categories') }}
    ),

    -- contains search brand mapping
    google_ads_keyword as (
        select * from {{ ref('google_ads_keyword') }}
    ),

--
    daily_keywords as (                                         -- every ad_group in this cte has assigned campaign id
        select
            campaign_id,
            ad_group_id,
            ad_group_criterion_keyword_text                                             as keyword,
            segments_date                                                               as ad_run_date,

            round((sum(metrics_cost_micros) / 1000000), 2)                              as spend,
            round(sum(metrics_impressions), 2)                                          as impressions,
            round(sum(metrics_clicks), 2)                                               as clicks,
        --    round(sum(metrics_video_views), 2)                                          as video_views,
            round(sum(metrics_conversions), 2)                                          as conversions,
            round(sum(metrics_conversions_value), 2)                                    as metrics_conversions_value

        from source_daily_google_ads_keywords

        group by
            campaign_id,
            ad_group_id,
            keyword,
            ad_run_date
    ),

    -- just to be sure about granularity
    daily_ad_groups as (                                        -- every ad_group in this cte has assigned campaign id
        select
            campaign_id,
            ad_group_id,
            segments_date                                                               as ad_run_date,

            round((sum(metrics_cost_micros) / 1000000), 2)                              as spend,
            round(sum(metrics_impressions), 2)                                          as impressions,
            round(sum(metrics_clicks), 2)                                               as clicks,
          --  round(sum(metrics_video_views), 2)                                          as video_views,
            round(sum(metrics_conversions), 2)                                          as conversions,
            round(sum(metrics_conversions_value), 2)                                    as metrics_conversions_value

        from source_daily_google_ads_ad_group_with_more_stats

        group by
            campaign_id,
            ad_group_id,
            ad_run_date
    ),

    daily_keywords_and_ad_groups as (
        select
            campaign_id,                    -- changed this dimension to be taken from both ad groups and keywords (now there will be no ad groups without campaign id)
            ad_group_id,
            keyword,
            ad_run_date,

            coalesce(
                daily_keywords.spend,
                daily_ad_groups.spend
            )                                                                           as spend,

            coalesce(
                daily_keywords.impressions,
                daily_ad_groups.impressions
            )                                                                           as impressions,

            coalesce(
                daily_keywords.clicks,
                daily_ad_groups.clicks
            )                                                                           as clicks,

            coalesce(
                daily_keywords.conversions,
                daily_ad_groups.conversions
            )                                                                           as conversions,

            coalesce(
                daily_keywords.metrics_conversions_value,
                daily_ad_groups.metrics_conversions_value
            )                                                                           as metrics_conversions_value

        from  daily_keywords
        full outer join  daily_ad_groups

        using (
            ad_run_date,
            ad_group_id,
            campaign_id                             -- added this dimension to join on
        )
    ),

    daily_campaigns as (
        select
            campaign_id,
            segments_date                                                               as ad_run_date,

            round((sum(metrics_cost_micros) / 1000000), 2)                              as spend,
            round(sum(metrics_impressions), 2)                                          as impressions,
            round(sum(metrics_clicks), 2)                                               as clicks,
            round(sum(metrics_video_views), 2)                                          as video_views,
            round(sum(metrics_conversions), 2)                                          as conversions,
            round(sum(metrics_conversions_value), 2)                                    as metrics_conversions_value

        from source_hourly_google_ads_campaigns

        group by
            campaign_id,
            ad_run_date
    ),

    daily_ad_groups_for_all_campaigns as (
        select
            campaign_id,
            ad_group_id,
            keyword,
            ad_run_date,

            coalesce(
                daily_ad_groups.spend,
                daily_campaigns.spend
            )                                                                           as spend,

            coalesce(
                daily_ad_groups.impressions,
                daily_campaigns.impressions
            )                                                                           as impressions,

            coalesce(
                daily_ad_groups.clicks,
                daily_campaigns.clicks
            )                                                                           as clicks,

            coalesce(
                daily_ad_groups.conversions,
                daily_campaigns.conversions
            )                                                                           as conversions,

            coalesce(
                daily_ad_groups.metrics_conversions_value,
                daily_campaigns.metrics_conversions_value
            )                                                                           as metrics_conversions_value

        from daily_keywords_and_ad_groups                                               as daily_ad_groups
        full outer join daily_campaigns

        using (
            ad_run_date,
            campaign_id
        )
    ),

    _join_dimensions_from_keywords as (
        select *
        from daily_ad_groups_for_all_campaigns
        left join google_ads_keyword

        using(
            campaign_id,
            ad_group_id,
            keyword
        )
    ),

    _join_dimensions_from_ad_campaings as (
        select *
        from _join_dimensions_from_keywords
        left join google_ads_campaigns
        using (campaign_id)
    ),

    _join_dimensions_from_ad_group as (
        select
            coalesce(
                entity_ad_group.category_from_destination_url_or_description,
                daily_ad_groups.category_from_campaign_name
            )                                                                           as product_category,

            daily_ad_groups.* except(category_from_campaign_name),

            entity_ad_group.* except(
                ad_group_id,
                category_from_destination_url_or_description
            )

        from _join_dimensions_from_ad_campaings                                         as daily_ad_groups
        left join
            intermediate_google_ads_group_with_product_categories                as entity_ad_group

        using (ad_group_id)
    ),

    _map_gads_type as (
        select
            case
                when brand_name = 'Victoria Beckham Beauty'                     then 'BRAND SEARCH'

                when
                        ad_group_type in (
                            '"SEARCH"',
                            'SEARCH_STANDARD'
                        )

                    or  campaign_advertising_channel_type = '"SEARCH"'
                                                                                then 'NON-BRAND SEARCH'

                when ad_group_type is null                                      then campaign_advertising_channel_type

                when ad_group_type in ('UNKNOWN','UNSPECIFIED')                 then campaign_advertising_channel_type

                when
                        ad_group_type like '%VIDEO%'
                    or  campaign_advertising_channel_type like '%VIDEO%'        then 'GOOGLE VIDEO'

                else ad_group_type

            end                                                                         as google_ads_category,

            *

        from _join_dimensions_from_ad_group
    ),

    _add_some_categories_info as (
        select
            REPLACE(
                google_ads_category,
                '"',
                ''
            )                                                                           as google_ads_category,

            coalesce(product_category, 'MSC')                                           as product_category,  -- to fill up the nulls in the table


            *   except (
                    google_ads_category,
                    product_category
                ),

                'Google Ads'                                                            as ads_platform,

            case
                when google_ads_category ='PERFORMANCE_MAX'                     then 'PMax'

                when google_ads_category in (
                    'BRAND SEARCH',
                    'NON-BRAND SEARCH'
                )                                                               then 'Paid Search'

                when google_ads_category in (
                    'VIDEO',
                    'VIDEO_RESPONSIVE'
                )                                                               then 'Paid Video'

                else 'gads not mapped'

            end                                                                         as channel


        from _map_gads_type
    ),

    _map_campaign_types as (
        select
            *,
            case
                when lower(campaign_name) like '%brand%'                    then 'brand'
                when lower(campaign_name) like '%pmax%'                     then 'pmax'
                                                                            else 'generic'
            end
                                                                                        as campaign_type
        from _add_some_categories_info
    )

-- final
        select * from _map_campaign_types