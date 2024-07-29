-- ./run_inside_docker.sh {{company_name}} {{company_id}} dbt run -m +intermediate_google_ads_ad_with_last_state -t dbt_transform

{{ config(materialized='view')}}

with

-- input
    source_daily_google_ads_ad as (
        select * from {{ source('sources_combined', 'source_daily_google_ads_ad') }}
    ),


    google_ads_ad_last_state as (
        -- cte_type:last_state
        select
            ad_group_id,
            ad_group_ad_ad_id                                                           as ad_id,

            max_by(
                ad_group_ad_ad_name,
                segments_date
            )                                                                           as ad_name,

            max_by(
               ad_group_ad_ad_final_urls,
               segments_date
            )                                                                           as final_urls,

            max_by(
               ad_group_ad_ad_type,
               segments_date
            )                                                                           as ad_type,

            max_by(
               ad_group_ad_ad_responsive_search_ad_descriptions,
               segments_date
            )                                                                           as responsive_search_ad_descriptions,

            max_by(
               ad_group_ad_ad_video_responsive_ad_descriptions,
               segments_date
            )                                                                           as video_responsive_ad_descriptions,

            max_by(
               ad_group_ad_ad_expanded_text_ad_description,
               segments_date
            )                                                                           as expanded_text_ad_description,

            max_by(
               ad_group_ad_ad_responsive_display_ad_descriptions,
               segments_date
            )                                                                           as responsive_display_ad_descriptions

        from source_daily_google_ads_ad

        group by
            ad_group_id,
            ad_id
    )

-- final
        select * from google_ads_ad_last_state

