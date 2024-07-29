-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +google_ads_campaigns+ -t dbt_transform

{{ config(materialized='view')}}

with
-- input
    source_hourly_google_ads_campaigns as (
        select * from {{ source('sources_combined','source_hourly_google_ads_campaigns') }}
    ),

    _spend_calc as (
        select
            metrics_cost_micros / 1000000                                               as spend,
            segments_date                                                               as ad_run_date,

            * except (
                metrics_cost_micros,
                segments_date
            )

        from source_hourly_google_ads_campaigns
    ),

    _group_by_campaign as (
        select
            campaign_id,
            max(segments_ad_network_type)                                               as segments_ad_network_type,
            max(to_json_string(metrics_interaction_event_types))                        as metrics_interaction_event_types,
            --max(to_json_string(campaign_bidding_strategy_type))                         as campaign_bidding_strategy_type,
            max(to_json_string(campaign_advertising_channel_type))                      as campaign_advertising_channel_type,
            max(campaign_payment_mode)                                                  as campaign_payment_mode,

            max_by(
                LOWER(campaign_name),
                ad_run_date
            )                                                                           as campaign_name,

            min(ad_run_date)                                                            as first_run_date,
            max(ad_run_date)                                                            as campaign_last_run_date,

            round(sum(spend))                                                           as campaign_total_spend,

            round(sum(
                CASE
                    WHEN ad_run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                    THEN spend
                    ELSE 0
                END
                ), 2)                                                                   as campaign_spend_in_last_7_days,

            round(sum(
                CASE
                    WHEN ad_run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                    THEN spend
                    ELSE 0
                END
                ), 2)                                                                   as campaign_spend_in_last_14_days


        from _spend_calc
        group by campaign_id
    ),

    _extract_country_from_campaign_name as (
        select
            UPPER(
                REGEXP_EXTRACT(
                    campaign_name,
                    r'(?i)(?:\[.*?\]\s*)*([a-z]{2,3})(?:[_-])'
                ))                                                                           as country,

            *

        from _group_by_campaign
    ),

    _extract_region_by_country_code as (
        select

            {{ map_country_to_region('country') }}                                          as region,

            case
                when country is null                                        then 'not parsed'
                when country = 'GB'                                         then 'UK'
                                                                            else country
            end                                                                             as country,

            * except (country)

        from _extract_country_from_campaign_name
    ),

    _extract_product_category_from_campaign_name as (
        select
            case
                when campaign_name like '%satin-kajal-liner%'                then 'SKL'

                when campaign_name like '%fragrance-discovery%'
                  or campaign_name like '%suite-302%'
                  or campaign_name like '%san-ysidro-drive%'
                  or campaign_name like '%portofino-97%'                     then 'FRD'

                when campaign_name like '%fragrance%'                        then 'FRG'

                when campaign_name like '%skin-augustinus-bader%'
                  or campaign_name like '%power-serum%'                      then 'SKA'

                when campaign_name like '%skin%'
                  or campaign_name like '%the-daily-cleansing-protocol%'     then 'SKN'

                when campaign_name like '%mascara%'                          then 'Mas'

                when campaign_name like '%lip%'
                  or campaign_name like '%gloss%'                            then 'LIP'

                when campaign_name like '%face%'
                  or campaign_name like '%reflect-highlighter-stick%'
                  or campaign_name like '%contour-stylus%'
                  or campaign_name like '%babyblade%'                        then 'FCE'

                when campaign_name like '%eye%'
                  or campaign_name like '%lid%'
                  or campaign_name like '%brightening-waterline-pencil%'     then 'EYE'

                                                                             else 'MSC'

            end                                                                         as category_from_campaign_name,

            *

        from _extract_region_by_country_code
    )


-- final
    select * from _extract_product_category_from_campaign_name